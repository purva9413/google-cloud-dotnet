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
using System;
using System.CodeDom;
using System.Collections.Concurrent;
using System.Linq;
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
    private Session _session;

    private readonly ConcurrentDictionary<TransactionConfiguration, Transaction> _executingTransactions =
            new ConcurrentDictionary<TransactionConfiguration, Transaction>();

    private readonly ConcurrentDictionary<TransactionConfiguration, (Task TransactionCreationTask, object newTransactionLock)> _transactionCreations = new ConcurrentDictionary<TransactionConfiguration, (Task TransactionCreationTask, object newTransactionLock)>();

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
    public SessionPoolOptions Options { get; }

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
    /// 
    /// </summary>
    /// <param name="client"></param>
    /// <param name="dbName"></param>
    /// <param name="dbRole"></param>
    public TargetedMultiplexSession(SpannerClient client, DatabaseName dbName, string dbRole)
    {
        Client = GaxPreconditions.CheckNotNull(client, nameof(client));
        //Options = GaxPreconditions.CheckNotNull(options, nameof(options)); // Are labels in SessionOptions needed ? SessionPoolOptions options
        _logger = client.Settings.Logger; // Just to avoid fetching it all the time
        _sessionCreateSemaphore = new SemaphoreSlim(1);

        DatabaseName = dbName;
        DatabaseRole = dbRole;

        _createSessionRequestTemplate = new CreateSessionRequest
        {
            DatabaseAsDatabaseName = DatabaseName,
            Session = new Session
            {
                //Labels = { Options.SessionLabels }, //TODO: Purva are these needed?
                CreatorRole = DatabaseRole ?? "",
                Multiplexed = true
            }
        };

        // TODO: Check if waiting on session creation makes sense or should be async/lazy load?
        Session = CreateMultiplexSessionsAsync(default).Result;
    }

    /// <summary>
    /// Executes an ExecuteBatchDml RPC asynchronously.
    /// </summary>
    /// <param name="request">The query request. Must not be null. The request will be modified with session and transaction details
    /// from this object. If this object's <see cref="TransactionId"/> is null, the request's transaction is not modified.</param>
    /// <param name="callSettings">If not null, applies overrides to this RPC call.</param>
    /// <param name="creationOptions"></param>
    /// <param name="isSingleUse"></param>
    /// <returns>A task representing the asynchronous operation. When the task completes, the result is the response from the RPC.</returns>
    public Task<ExecuteBatchDmlResponse> ExecuteBatchDmlAsync(ExecuteBatchDmlRequest request, CallSettings callSettings, TransactionOptions creationOptions, bool isSingleUse)
    {
        CheckNeedsRefresh();
        GaxPreconditions.CheckNotNull(request, nameof(request));

        request.SessionAsSessionName = SessionName;

        return ExecuteMaybeWithTransactionSelectorAsync(
            transactionSelectorSetter: SetCommandTransaction,
            commandAsync: ExecuteBatchDmlAsync,
            inlinedTransactionExtractor: GetInlinedTransaction,
            skipTransactionCreation: false,
            callSettings?.CancellationToken ?? default, creationOptions, isSingleUse);

        void SetCommandTransaction(TransactionSelector transactionSelector) => request.Transaction = transactionSelector;

        Task<ExecuteBatchDmlResponse> ExecuteBatchDmlAsync() => RecordSuccessAndExpiredSessions(Client.ExecuteBatchDmlAsync(request, callSettings));

        Transaction GetInlinedTransaction(ExecuteBatchDmlResponse response) => response?.ResultSets?.FirstOrDefault()?.Metadata?.Transaction;
    }

    /// <summary>
    /// Executes a Commit RPC asynchronously.
    /// </summary>
    /// <param name="request">The commit request. Must not be null. The request will be modified with session and transaction details
    /// from this object.</param>
    /// <param name="callSettings">If not null, applies overrides to this RPC call.</param>
    /// <param name="creationOptions"></param>
    /// <param name="isSingleUse"></param>
    /// <returns>A task representing the asynchronous operation. When the task completes, the result is the response from the RPC.</returns>
    public Task<CommitResponse> CommitAsync(CommitRequest request, CallSettings callSettings, TransactionOptions creationOptions, bool isSingleUse)
    {
        CheckNeedsRefresh();
        GaxPreconditions.CheckNotNull(request, nameof(request));

        request.SessionAsSessionName = SessionName;

        return ExecuteMaybeWithTransactionSelectorAsync(
            transactionSelectorSetter: SetCommandTransaction,
            commandAsync: CommitAsync,
            inlinedTransactionExtractor: null, // Commit does not support inline transactions.
            skipTransactionCreation: request.Mutations.Count == 0, // If there are only mutations we won't have a transaction but we need one.
            callSettings?.CancellationToken ?? default, creationOptions, isSingleUse);

        void SetCommandTransaction(TransactionSelector transactionSelector)
        {
            switch (transactionSelector.SelectorCase)
            {
                case TransactionSelector.SelectorOneofCase.Id:
                    request.TransactionId = transactionSelector.Id;
                    break;
                case TransactionSelector.SelectorOneofCase.Begin:
                    throw new InvalidOperationException("Commit does not support inline transactions. This is a bug in library code.");
                case TransactionSelector.SelectorOneofCase.SingleUse:
                    throw new InvalidOperationException("A single use transaction cannot be committed.");
                default:
                    throw new InvalidOperationException("Cannot commit a PooledSession with no associated transaction");
            }
        }

        async Task<CommitResponse> CommitAsync()
        {
            // If a transaction had been started, by now SetTransaction should have been called with a transaction ID.
            // If not, there's an attempt to commit a non-existent transaction.
            if (request.TransactionId is null || request.TransactionId.IsEmpty)
            {
                throw new InvalidOperationException("A transaction has not been acquired for this Session because no command execution has been attempted.");
            }

            var response = await RecordSuccessAndExpiredSessions(Client.CommitAsync(request, callSettings)).ConfigureAwait(false);
            MarkAsCommittedOrRolledBack(); // Purva: This might not be necessary with Mux as txn were marked as commited/rollback in PooledSession for proper handling when PooledSession was retured back the pool. This is not a case in Mux, we will just refresh the underlying session of the mux.
            return response;
        }
    }

    /// <summary>
    /// Executes a Rollback RPC asynchronously.
    /// </summary>
    /// <param name="request">The rollback request. Must not be null. The request will be modified with session and transaction details
    /// from this object.</param>
    /// <param name="callSettings">If not null, applies overrides to this RPC call.</param>
    /// <param name="txnCreationOptions"></param>
    /// <param name="isSingleUse"></param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task RollbackAsync(RollbackRequest request, CallSettings callSettings, TransactionOptions txnCreationOptions, bool isSingleUse)
    {
        CheckNeedsRefresh();
        
        GaxPreconditions.CheckNotNull(request, nameof(request));

        request.SessionAsSessionName = SessionName;

        return ExecuteMaybeWithTransactionSelectorAsync(
            transactionSelectorSetter: SetCommandTransaction,
            commandAsync: RollbackAsync,
            inlinedTransactionExtractor: null, // Rollback does not support inline transactions.
            skipTransactionCreation: true, // If there's no transaction by the time roll back is called, we fail, we don't need to create one.
            callSettings?.CancellationToken ?? default, txnCreationOptions, isSingleUse);

        void SetCommandTransaction(TransactionSelector transactionSelector)
        {
            switch (transactionSelector.SelectorCase)
            {
                case TransactionSelector.SelectorOneofCase.Id:
                    request.TransactionId = transactionSelector.Id;
                    break;
                case TransactionSelector.SelectorOneofCase.Begin:
                    throw new InvalidOperationException("Rollback does not support inline transactions. This is a bug in library code.");
                case TransactionSelector.SelectorOneofCase.SingleUse:
                    throw new InvalidOperationException("A single use transaction cannot be rolled back.");
                default:
                    throw new InvalidOperationException("Cannot roll back a PooledSession with no associated transaction");
            }
        }

        async Task<bool> RollbackAsync()
        {
            // If a transaction had been started, by now SetTransaction should have been called with a transaction ID.
            // If not, there's an attempt to roll back a transaction that was never started.
            // Possibly starting the transaction is what failed, but if we fail here as well, we are passing the burden to calling code
            // to know whether a transaction was actually acquired before calling rollback, and we don't want to do that.
            // Attemting to roll back an empty transaction is no-op.
            if (request.TransactionId is null || request.TransactionId.IsEmpty)
            {
                return false;
            }

            await RecordSuccessAndExpiredSessions(Client.RollbackAsync(request, callSettings)).ConfigureAwait(false);
            MarkAsCommittedOrRolledBack();
            // Just so we can use the same ExecuteMaybeWithTransactionAsync method that expects a result.
            return true;
        }
    }

    internal ReliableStreamReader ExecuteReadOrQueryStreamReader(ReadOrQueryRequest request, CallSettings callSettings, TransactionOptions creationOptions, bool isSingleUse)
    {
        CheckNeedsRefresh();
        GaxPreconditions.CheckNotNull(request, nameof(request));

        request.SessionAsSessionName = SessionName;
        SpannerClientImpl.ApplyResourcePrefixHeaderFromSession(ref callSettings, request.Session);
        Client.MaybeApplyRouteToLeaderHeader(ref callSettings, creationOptions.ModeCase);
        MaybeApplyDirectedReadOptions(request.UnderlyingRequest, creationOptions);

        ResultStream stream = new ResultStream(Client, request, this, callSettings, creationOptions, isSingleUse);
        return new ReliableStreamReader(stream, Client.Settings.Logger);
    }

    private void MaybeApplyDirectedReadOptions(IReadOrQueryRequest request, TransactionOptions creationOptions)
    {
        if (creationOptions.ModeCase == ModeOneofCase.ReadOnly // Directed reads apply only to single use or read only transactions. Single use are read only.
            && request.DirectedReadOptions is null) // Request specific options have priority over client options.
        {
            request.DirectedReadOptions = Client.Settings.DirectedReadOptions;
        }

        // We don't validate that DirectedReadOptions is null when this is a non-read-only transaction.
        // We just pass the request along as we received it. The service should fail if there are options set.
        // This was agreed as part of the client library desing.
    }

    internal Task<PartitionResponse> PartitionReadOrQueryAsync(PartitionReadOrQueryRequest request, CallSettings callSettings, TransactionOptions creationOptions, bool isSingleUse) // Taken from PooledSession
    {
        CheckNeedsRefresh();
        GaxPreconditions.CheckNotNull(request, nameof(request));

        request.SessionAsSessionName = SessionName;

        return ExecuteMaybeWithTransactionSelectorAsync(
            transactionSelectorSetter: SetCommandTransaction,
            commandAsync: PartitionReadOrQueryAsync,
            inlinedTransactionExtractor: GetInlinedTransaction,
            skipTransactionCreation: false,
            callSettings?.CancellationToken ?? default, creationOptions, isSingleUse);

        void SetCommandTransaction(TransactionSelector transactionSelector)
        {
            switch (transactionSelector.SelectorCase)
            {
                case TransactionSelector.SelectorOneofCase.Id:
                case TransactionSelector.SelectorOneofCase.Begin:
                    request.Transaction = transactionSelector;
                    break;
                case TransactionSelector.SelectorOneofCase.SingleUse:
                    throw new InvalidOperationException("A single use transaction cannot be used for creating partitioned reads or queries.");
                default:
                    throw new InvalidOperationException("Cannot call PartitionReadOrQueryAsync with no associated transaction.");
            }
        }

        Task<PartitionResponse> PartitionReadOrQueryAsync()
        {
            // By now SetTransaction should have been called with a valid transaction selector.
            // If not, there's a bug in code because we said not to skip transaction creation.
            if (request.Transaction is null)
            {
                throw new InvalidOperationException("Cannot call PartitionReadOrQueryAsync with no associated transaction.");
            }

            return RecordSuccessAndExpiredSessions(request.PartitionAsync(Client, callSettings));
        }

        Transaction GetInlinedTransaction(PartitionResponse response) => response?.Transaction;
    }

    internal async Task<TResponse> ExecuteMaybeWithTransactionSelectorAsync<TResponse>(
            Action<TransactionSelector> transactionSelectorSetter,
            Func<Task<TResponse>> commandAsync,
            Func<TResponse, Transaction> inlinedTransactionExtractor,
            bool skipTransactionCreation,
            CancellationToken cancellationToken,
            TransactionOptions txnCreationOptions, bool isSingleUse
            )
    {
        // If this session is configured to use no transaction we just execute the command.
        if (txnCreationOptions.ModeCase == TransactionOptions.ModeOneofCase.None)
        {
            return await commandAsync().ConfigureAwait(false);
        }

        // If this is to be a single use transaction, we set the selector to single use and execute the command.
        if (isSingleUse)
        {
            transactionSelectorSetter(new TransactionSelector { SingleUse = txnCreationOptions });
            return await commandAsync().ConfigureAwait(false);
        }

        // If we already have a transaction ID, we set the selector to that and execute the command.
        // TransactionId is accessed and modified via Interlock.CompareExchange so these two are atomic operations.
        // But also, if TransactionId is about to be modified right after this check, that's not a problem, because next
        // we'll be awaiting on the task that does the modifying.
        if (GetTransaction(txnCreationOptions, isSingleUse)?.Id is ByteString transactionId)
        {
            transactionSelectorSetter(new TransactionSelector { Id = transactionId });
            return await commandAsync().ConfigureAwait(false);
        }

        // We now know that we don't have a transaction ID but we need one to execute
        // the command we have been given.
        // We now need to check if we are already creating a transaction or not.
        // If we are, we just get ready to wait for it.
        // If we are not, but we need to, we start and save the task that does so,
        // and get ready to wait for it.

        // This is the function that will wait for the transaction task to be done.
        // We need to initialize a function within the lock, so we can execute
        // async code, outside the lock.
        // We initialize it with just command, in case no transaction is being created
        // and the caller does not require one. This is the case for commits and rollbacks
        // executed when no transaction has been created before.
        // Commits and rollbacks will know how to handle transaction absence.
        Func<Task<TResponse>> commandMaybeWithTransactionAsync = commandAsync;

        TransactionConfiguration transactionConfiguration = new TransactionConfiguration(txnCreationOptions, isSingleUse);
        object transactionCreationLock = _transactionCreations.GetOrAdd(transactionConfiguration, (TransactionCreationTask: null, newTransactionLock: new object()));
        lock (transactionCreationLock)
        {
            var transactionCreationTask = _transactionCreations[transactionConfiguration].TransactionCreationTask;
            // We are not creating a transaction. We might need to do so.
            if (transactionCreationTask is null)
            {
                // We need to create a transaction.
                if (!skipTransactionCreation)
                {
                    // The calling command does not support inlining
                    // or the transaction mode Partitioned DML, which cannot be inline.
                    // Either way we need to create a transaction explicitly.
                    if (inlinedTransactionExtractor is null || txnCreationOptions.ModeCase == ModeOneofCase.PartitionedDml)
                    {
                        transactionCreationTask = Task.Run(() => SetExplicitTransactionAsync(cancellationToken), cancellationToken);
                        commandMaybeWithTransactionAsync = () => CommandWithTransactionAsync(cancellationToken);
                    }
                    // The calling command supports inlining.
                    // We attempt inlining but if that fails, we create a transaction explicitly.
                    else
                    {
                        // Create a task for executing the command which inlines transaction creation.
                        // If this task succeeds we'll have both the transaction ID and the response from the command.
                        // If this task fails we'll have to attempt to begin a transaction explicitly, which will give us a transaction ID,
                        // and then we'll have to execute the command with that transaction.
                        Task<TResponse> commandWithInliningTask = Task.Run(CommandWithInliningAsync, cancellationToken);

                        // Now, create two tasks, one that is done when we have a transaction ID (via inlining or explicit),
                        // and one that is done when the command is done (with inlining or explicit).

                        // The transaction creation task is the combination of attempting inlining,
                        // and if that fails, explicitly creating a transaction.
                        transactionCreationTask = Task.Run(async () =>
                        {
                            try
                            {
                                await SetInlinedTransactionAsync(commandWithInliningTask).ConfigureAwait(false);
                            }
                            catch (Exception ex)
                            {
                                Client.Settings.Logger.Warn("Transaction creation via inlining failed. " +
                                    "Attempting to begin an explicit transaction.",
                                    ex);
                                await SetExplicitTransactionAsync(cancellationToken).ConfigureAwait(false);
                            }
                        }, cancellationToken);

                        // The command execution task is the combination of attempting inlining,
                        // and if that fails, executing the command with the explicitly created transaction.
                        commandMaybeWithTransactionAsync = async () =>
                        {
                            try
                            {
                                var response = await commandWithInliningTask.ConfigureAwait(false);
                                // If we are here, inlining was successful so we have a transaction.
                                // We only wait for the _transactionCreationTaks to be done to guarantee
                                // that the transaction ID obtained via inlining has been stored and can
                                // be access via TransactionId. In turn this guarantees command executors
                                // that there's an ID available inmediately after a successful command execution.
                                await transactionCreationTask.ConfigureAwait(false);
                                return response;
                            }
                            catch (Exception ex)
                            {
                                Client.Settings.Logger.Warn("Command execution with transaction inlining failed. " +
                                    "Waiting for an explicit transaction to be created to attempt command execution.",
                                    ex);
                                // If we got here, transaction inlining (plus command execution) failed.
                                // That means that _transactionCreationTask is attempting to created an explicit
                                // transaction.
                                // So now we wait for that transaction to be created and the execute the command normally.
                                return await CommandWithTransactionAsync(cancellationToken).ConfigureAwait(false);
                            }
                        };
                    }
                }

                // No transaction is being created, but we don't need to do so.
                // commandWithTransactionAsync is already initialized to just commandAsync.
            }
            // We are creating a transaction, let's get ready to wait for that to be done and use it.
            else
            {
                commandMaybeWithTransactionAsync = () => CommandWithTransactionAsync(cancellationToken);
            }

            // Update dictionary with first set task and lock which will be used for all commands run as part of the same transaction
            _transactionCreations[transactionConfiguration] = (TransactionCreationTask: transactionCreationTask, newTransactionLock: transactionCreationLock);
        }

        return await commandMaybeWithTransactionAsync().ConfigureAwait(false);

        async Task<TResponse> CommandWithTransactionAsync(CancellationToken cancellationToken)
        {
            // This is only called when we know the transactionCreationTask for the specific TransactionConfiguration has been initialized in the block above
            var transactionCreationTask = _transactionCreations[transactionConfiguration].TransactionCreationTask;
            await transactionCreationTask.WithCancellationToken(cancellationToken).ConfigureAwait(false);
            // Now we know there's a transaction id.
            transactionSelectorSetter(new TransactionSelector { Id = GetTransaction(txnCreationOptions, isSingleUse)?.Id });
            return await commandAsync().ConfigureAwait(false);
        }

        Task<TResponse> CommandWithInliningAsync()
        {
            transactionSelectorSetter(new TransactionSelector { Begin = txnCreationOptions });
            return commandAsync();
        }

        async Task SetExplicitTransactionAsync(CancellationToken cancellationToken)
        {
            Transaction transaction = await BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            SetTransaction(transaction);
        }

        async Task SetInlinedTransactionAsync(Task<TResponse> commandWithInliningTask)
        {
            TResponse response = await commandWithInliningTask.ConfigureAwait(false);
            Transaction transaction = inlinedTransactionExtractor(response)
                ?? throw new InvalidOperationException("The inlined transaction extractor returned a null transaction. " +
                "This is possibly because of a bug in library code or because an operation that supported transaction inlining has stopped doing so.");
            SetTransaction(transaction);
        }

        void SetTransaction(Transaction transaction)
        {
            if(!_executingTransactions.TryAdd(transactionConfiguration, transaction))
            {
                Console.WriteLine($"Warning!! There was an issue adding the new transaction to the dictionary, {txnCreationOptions}");
            }
            //if (Interlocked.CompareExchange(ref _transaction, transaction, null) is not null)
            //{
            //    throw new InvalidOperationException("This session already contains a transaction. This is a bug in library code.");
            //}
        }

        Task<Transaction> BeginTransactionAsync(CancellationToken cancellationToken)
        {
            var request = new BeginTransactionRequest
            {
                Options = txnCreationOptions,
                SessionAsSessionName = SessionName,
            };
            var callSettings = Client.Settings.BeginTransactionSettings
                .WithExpiration(Expiration.FromTimeout(Options.Timeout))
                .WithCancellationToken(cancellationToken);
            return RecordSuccessAndExpiredSessions(Client.BeginTransactionAsync(request, callSettings));
        }
    }

    private async Task<T> RecordSuccessAndExpiredSessions<T>(Task<T> task)
    {
        var result = await task.WithSessionExpiryChecking(Session).ConfigureAwait(false);
        MaybeMarkMuxForRefresh();
        return result;
    }

    private async Task RecordSuccessAndExpiredSessions(Task task)
    {
        await task.WithSessionExpiryChecking(Session).ConfigureAwait(false);
        MaybeMarkMuxForRefresh();
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

    private async Task RefreshMuxSession()
    {
        if(Session.Expired)
        {
            // TODO: How to check if _session has no executing transactions before exchanging?
            // One way to do this is maintain a temporary second freshSession which will be null except during interim time between refresh _session with freshSession
            Session freshSession = await CreateMultiplexSessionsAsync(default).ConfigureAwait(false);
            _ = Interlocked.Exchange(ref _session, freshSession);
        }
    }

    /// <summary>
    /// Creates a <see cref="ReliableStreamReader"/> for the given request.
    /// </summary>
    /// <param name="request">
    /// The query request. Must not be null.
    /// Will be modified to include session information from this pooled session.
    /// May be modified to include transaction and directed read options information
    /// from this pooled session and its underlying <see cref="SpannerClient"/>.
    /// </param>
    /// <param name="callSettings">If not null, applies overrides to this RPC call.</param>
    /// <param name="transactionOptions"></param>
    /// <param name="isSingleUse"></param>
    /// <returns>A <see cref="ReliableStreamReader"/> for the streaming SQL request.</returns>
    public ReliableStreamReader ExecuteSqlStreamReader(ExecuteSqlRequest request, CallSettings callSettings, TransactionOptions transactionOptions, bool isSingleUse) =>
        ExecuteReadOrQueryStreamReader(ReadOrQueryRequest.FromRequest(request), callSettings, transactionOptions, isSingleUse);

    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionOptions"></param>
    /// <param name="singleUseTransaction"></param>
    /// <returns></returns>
    public Transaction GetTransaction(TransactionOptions transactionOptions, bool singleUseTransaction)
    {
        Transaction foundTransaction;
        if (_executingTransactions.TryGetValue(new TransactionConfiguration(transactionOptions, singleUseTransaction), out foundTransaction))
        {
            return foundTransaction;
        }

        return null;
    }

    private async Task<Session> CreateMultiplexSessionsAsync(CancellationToken cancellationToken)
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
                multiplexSession = await Client.CreateSessionAsync(createSessionRequest, cancellationToken).ConfigureAwait(false);

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

    internal class TransactionConfiguration
    {
        internal TransactionOptions _creationOptions;
        internal Boolean _singleUse;

        public TransactionConfiguration(TransactionOptions transactionOptions, bool singleUse)
        {
            _creationOptions = transactionOptions;
            _singleUse = singleUse;
        }

        public override bool Equals(object obj) => Equals(obj as TransactionConfiguration);

        public bool Equals(TransactionConfiguration other) => _creationOptions.Equals(other._creationOptions) && _singleUse == other._singleUse;

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 31;
                hash = hash * 23 + (_creationOptions?.GetHashCode() ?? 0);
                hash = hash * 23 + _singleUse.GetHashCode();
                return hash;
            }
        }

    }
}
