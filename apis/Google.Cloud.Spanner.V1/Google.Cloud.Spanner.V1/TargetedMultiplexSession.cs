// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License"):
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.V1.Internal;
using Google.Cloud.Spanner.V1.Internal.Logging;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using System;
using System.CodeDom;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Management;
using System.Threading;
using System.Threading.Tasks;
using static Google.Cloud.Spanner.V1.TransactionOptions;

namespace Google.Cloud.Spanner.V1;

/// <summary>
/// TODO: Add summary for mux sessions
/// </summary>
public class TargetedMultiplexSession
{
    private readonly SemaphoreSlim _sessionCreateSemaphore;
    private readonly Logger _logger;
    private readonly CreateSessionRequest _createSessionRequestTemplate;
    internal Session _session;

    /// <summary>
    /// The client used for all operations in this multiplex session.
    /// </summary>
    internal SpannerClient Client { get; }

    internal Task<Session> CreateSessionTask { get; }

    /// <summary>
    /// The name of the session. This is never null.
    /// </summary>
    public SessionName SessionName => Session.SessionName;

    /// <summary>
    /// The Spanner session resource associated to this pooled session.
    /// Won't be null.
    /// </summary>
    internal Session Session
    {
        get { return _session; }
        private set { _session = value; }
    }

    /// <summary>
    /// The options governing this multiplex session.
    /// </summary>
    public MultiplexSessionOptions Options { get; }

    /// <summary>
    /// The database for this multiplex session
    /// </summary>
    public DatabaseName DatabaseName { get; }

    /// <summary>
    /// The database role of the multiplex session
    /// </summary>
    public string DatabaseRole { get; }
    private bool NeedsRefresh { get; set; }

    /// <summary>
    /// Indicates whether the server has told us that the session has expired.
    /// </summary>
    internal bool ServerExpired => Session.Expired;

    /// <summary>
    /// 
    /// </summary>
    /// <param name="client"></param>
    /// <param name="dbName"></param>
    /// <param name="dbRole"></param>
    /// <param name="options"></param>
    public TargetedMultiplexSession(SpannerClient client, DatabaseName dbName, string dbRole, MultiplexSessionOptions options)
    {
        Client = GaxPreconditions.CheckNotNull(client, nameof(client));
        Options = options ?? new MultiplexSessionOptions();
        _logger = client.Settings.Logger; // Just to avoid fetching it all the time
        _sessionCreateSemaphore = new SemaphoreSlim(1);

        DatabaseName = dbName;
        DatabaseRole = dbRole;

        _createSessionRequestTemplate = new CreateSessionRequest
        {
            DatabaseAsDatabaseName = DatabaseName,
            Session = new Session
            {
                Labels = { Options.SessionLabels },
                CreatorRole = DatabaseRole ?? "",
                Multiplexed = true
            }
        };
    }

    private void MaybeMarkMuxForRefresh()
    {
        if (Session.Expired) // TODO: Maybe add polling of 7 days and refresh
        {
            NeedsRefresh = true;
        }
    }

    private void CheckNeedsRefresh()
    {
        if (NeedsRefresh)
        {
            throw new ObjectDisposedException($"Multiplex Session for {SessionName} needs to be refreshed, and cannot be reused.");
        }
    }

    private async Task<Boolean> UpdateMuxSession()
    {
        Session oldSession = _session;
        // TODO: How to check if _session has no executing transactions before exchanging?
        // One way to do this is maintain a temporary second freshSession which will be null except during interim time between refresh _session with freshSession
        Session freshSession = await CreateSessionsAsync(default).ConfigureAwait(false);

        Interlocked.Exchange(ref _session, freshSession);

        return _session != oldSession;
    }

    internal void MaybeRefreshWithTimePeriodCheck()
    {
        
        DateTime currentTime = DateTime.Now;
        DateTime sessionCreateTime = Session.CreateTime.ToDateTime();

        if (Session.Expired || currentTime - sessionCreateTime >= TimeSpan.FromDays(28))
        {
            // If the session has expired on a client RPC request call, or has exceeded the 28 day Mux session refresh guidance
            // No request can proceed without us having a new Session to work with
            // Block on refreshing and getting a new session
            bool sessionIsRefreshed = UpdateMuxSession().Result;

            if(!sessionIsRefreshed)
            {
                throw new Exception("Unable to refresh multiplex session, and the old session has expired or is 28 days past refresh"); 
            }

            _logger.Info($"Refreshed session since it was expired or past 28 days refresh period. New session {SessionName}");
        }

            if (currentTime - sessionCreateTime > TimeSpan.FromDays(7))
        {
            // The Mux sessions have a lifespan of 28 days. We check if we need a session refresh in every request needing the session
            // If the timespan of a request needing a session and the session creation time is greater than 7 days, we proactively refresh the mux session
            // The request can safely use the older session since it is still valid while we do this refresh to fetch the new session.
            // Hence fire and forget the session refresh.
            _ = Task.Run(UpdateMuxSession);
        }
    }

    private async Task<Session> CreateSessionsAsync(CancellationToken cancellationToken)
    {
        try
        {
            var callSettings = Client.Settings.CreateSessionSettings
                .WithExpiration(Expiration.FromTimeout(Options.Timeout))
                .WithCancellationToken(cancellationToken);

            CreateSessionRequest createSessionRequest = _createSessionRequestTemplate.Clone(); // TODO: Check if cloning is necessary since we are ideally only creating 1 mux per client

            Session multiplexSession;

            bool acquiredSemaphore = false;
            try
            {
                await _sessionCreateSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                acquiredSemaphore = true;

                multiplexSession = await Client.CreateSessionAsync(createSessionRequest, callSettings).ConfigureAwait(false);

                return multiplexSession;

            }
            catch (OperationCanceledException)
            {
                _logger.Warn(() => $"Creation request cancelled before we could procure a Multiplex Session for DatabaseName: {DatabaseName}, DatabaseRole: {DatabaseRole}");
                throw;
            }
            finally
            {
                if (acquiredSemaphore)
                {
                    _sessionCreateSemaphore.Release();
                }
            }
        }
        catch (Exception e)
        {
            _logger.Warn(() => $"Failed to create multiplex session for DatabaseName: {DatabaseName}, DatabaseRole: {DatabaseRole}", e);
            throw;
        }
        finally
        {
            // Nothing to do here since for legacy SessionPool we had to have some logging for when the pool went from healthy to unhealthy.
            // This could be mean n number of things went wrong in the pool
            // But we the MUX session, we essentially only have 1 session we need to manage per client so there is no case of the mux session going back and forth in terms of its healthiness.
        }

    }

    /// <summary>
    /// 
    /// </summary>
    public sealed partial class MultiplexSessionBuilder
    {
        /// <summary>
        /// 
        /// </summary>
        public MultiplexSessionBuilder()
        {
        }

        /// <summary>
        /// The options governing this multiplex session.
        /// </summary>
        public MultiplexSessionOptions Options { get; }

        /// <summary>
        /// The database for this multiplex session
        /// </summary>
        public DatabaseName DatabaseName { get; }

        /// <summary>
        /// The database role of the multiplex session
        /// </summary>
        public string DatabaseRole { get; }

        /// <summary>
        /// The client used for all operations in this multiplex session.
        /// </summary>
        internal SpannerClient Client { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<TargetedMultiplexSession> BuildAsync(CancellationToken cancellationToken = default)
        {
            TargetedMultiplexSession targetedMultiplexSession = new TargetedMultiplexSession(Client, DatabaseName, DatabaseRole, Options);

            await targetedMultiplexSession.CreateSessionsAsync(cancellationToken).ConfigureAwait(false);

            return targetedMultiplexSession;
        }
    }
}
