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
using Google.Api.Gax.Testing;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.V1;
using Google.Cloud.Spanner.V1.Internal.Logging;
using Google.Protobuf.WellKnownTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using static Google.Cloud.Spanner.V1.MultiplexSession;

namespace Google.Cloud.Spanner.V1.Tests;
public class MultiplexSessionTests
{
    private const string testDatabase = "projects/testproject/instances/testinstance/databases/testdb";
    private const string testSession = "projects/testproject/instances/testinstance/databases/testdb/sessions/multiplexSession-1234-fcds-4567-xyz";

    [Fact]
    public async Task TestBuilderCreation()
    {
        MultiplexSession multiplexSession = await FetchTestMultiplexSession();

        Assert.NotNull(multiplexSession);
        Assert.NotNull(multiplexSession.Session);
        Assert.NotNull(multiplexSession.Client);
        Assert.NotNull(multiplexSession.DatabaseName);
        Assert.NotNull(multiplexSession.DatabaseRole);
    }

    [Fact]
    public async Task TestSessionHasExpired()
    {
        SpannerClient fakeClient = CreateFakeClient();
        MultiplexSession multiplexSession = await FetchTestMultiplexSession(fakeClient);

        DateTime sessionCreateTime = multiplexSession.Session.CreateTime.ToDateTime();
        FakeClock clock = (FakeClock) fakeClient.Settings.Clock;

        clock.AdvanceTo(sessionCreateTime+TimeSpan.FromDays(3));
        Assert.True(multiplexSession.SessionHasExpired(2.0));

        clock.AdvanceTo(sessionCreateTime + TimeSpan.FromDays(7));
        Assert.True(multiplexSession.SessionHasExpired());
    }

    private SpannerClient CreateFakeClient()
    {
        SpannerClient fakeClient = SpannerClientHelpers.CreateMockClient(Logger.DefaultLogger);
        fakeClient.SetupMultiplexSessionCreationAsync();

        return fakeClient;
    }

    private async Task<MultiplexSession> FetchTestMultiplexSession(SpannerClient client = null)
    {
        if (!DatabaseName.TryParse(testDatabase, out var databaseName))
        {
            throw new Exception($"Unable to parse string to DatabaseName {testDatabase}");
        }

        if (client == null) {
            client = CreateFakeClient();
        }

        MultiplexSessionBuilder builder = new MultiplexSessionBuilder(databaseName, client)
        {
            DatabaseRole = "testRole",
        };

        MultiplexSession multiplexSession = await builder.BuildAsync();

        return multiplexSession;
    }
    //private class FakeSpannerClient : SpannerClient
    //{
    //    private readonly SpannerClient _mockClient;

    //    public Logger Logger { get; } = Logger.DefaultLogger;
    //    private FakeClock Clock => (FakeClock) _mockClient.Settings.Clock;
    //    // A simple non-counting factory.
    //    public FakeSpannerClient()
    //    {
    //        _mockClient = SpannerClientHelpers.CreateMockClient(Logger);
    //    }

    //    public override Task<Session> CreateSessionAsync(CreateSessionRequest request, CallSettings callSettings = null)
    //    {
    //        SessionName.TryParse(testSession, out var sessionName);
    //        Assert.NotNull(request.DatabaseAsDatabaseName);
    //        Session response = new Session();
    //        response.CreateTime = Timestamp.FromDateTime(Clock.GetCurrentDateTimeUtc());
    //        response.CreatorRole = request.Session.CreatorRole;
    //        response.Multiplexed = request.Session.Multiplexed;
    //        response.SessionName = sessionName;

    //        return Task.FromResult(response);
    //    }
    //}
}
