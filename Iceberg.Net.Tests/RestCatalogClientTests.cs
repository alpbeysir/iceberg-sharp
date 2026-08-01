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

    [Theory]
    [InlineData(HttpStatusCode.BadRequest)]
    [InlineData((HttpStatusCode)418)]
    public async Task ErrorResponseMessageIsIncludedInExceptionForEveryErrorPath(HttpStatusCode statusCode)
    {
        const string responseBody =
            """
            {
              "message": "Suspicious Path Character",
              "url": "http://localhost:8181/v1/namespaces/test%1Ftest_child/tables?pageSize=10",
              "status": "400"
            }
            """;
        using HttpClient httpClient = new(new ErrorResponseHandler(responseBody, statusCode));
        RestCatalogClient client = new(httpClient) { BaseUrl = "http://localhost:8181/v1/" };

        Func<Task> request = () => client.ListTablesAsync("test\u001ftest_child", pageSize: 10);

        var assertion = await request.Should().ThrowAsync<IcebergRestException>();
        assertion.Which.Message.Should().StartWith("Suspicious Path Character");
        assertion.Which.ServerMessage.Should().Be("Suspicious Path Character");
        assertion.Which.Response.Should().Be(responseBody);
    }

    private sealed class ErrorResponseHandler(string responseBody, HttpStatusCode statusCode) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            HttpResponseMessage response = new(statusCode)
            {
                Content = new StringContent(responseBody, Encoding.UTF8, "application/json"),
                RequestMessage = request
            };
            return Task.FromResult(response);
        }
    }
}
