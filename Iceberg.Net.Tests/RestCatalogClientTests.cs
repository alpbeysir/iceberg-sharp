using System.Net;
using System.Text;
using AwesomeAssertions;
using Iceberg.Net.Catalog;
using Iceberg.Net.Rest;

namespace Iceberg.Net.Tests;

public class RestCatalogClientTests
{
    [Fact]
    public void ConfiguredNamespaceSeparatorIsDecodedBeforeEncodingNamespaces()
    {
        CatalogConfig response = new(
            new Dictionary<string, string>(),
            [],
            null,
            new Dictionary<string, string> { ["namespace-separator"] = "%2E" });
        UserConfig userConfig = new() { BaseUrl = "http://localhost:8181/v1/" };
        TypedCatalogConfig config = new(response, userConfig);
        Identifier identifier = ["test", "test_child"];

        identifier.GetEncoded(config.NamespaceSeparator).Should().Be("test.test_child");
    }

    [Fact]
    public async Task ErrorResponseMessageIsIncludedInException()
    {
        const string responseBody =
            """
            {
              "message": "Suspicious Path Character",
              "url": "http://localhost:8181/v1/namespaces/test%1Ftest_child/tables?pageSize=10",
              "status": "400"
            }
            """;
        using HttpClient httpClient = new(new ErrorResponseHandler(responseBody));
        RestCatalogClient client = new(httpClient) { BaseUrl = "http://localhost:8181/v1/" };

        Func<Task> request = () => client.ListTablesAsync("test\u001ftest_child", pageSize: 10);

        var assertion = await request.Should().ThrowAsync<IcebergRestException>();
        assertion.Which.Message.Should().Contain("Suspicious Path Character");
        assertion.Which.Response.Should().Be(responseBody);
    }

    private sealed class ErrorResponseHandler(string responseBody) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            HttpResponseMessage response = new(HttpStatusCode.BadRequest)
            {
                Content = new StringContent(responseBody, Encoding.UTF8, "application/json"),
                RequestMessage = request
            };
            return Task.FromResult(response);
        }
    }
}
