using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Iceberg.Net.Rest.OAuth;

// ReSharper disable UnusedAutoPropertyAccessor.Global

#pragma warning disable 1573 // Disable "CS1573 Parameter '...' has no matching param tag in the XML comment for ...
#pragma warning disable 1591 // Disable "CS1591 Missing XML comment for publicly visible type or member ..."

namespace Iceberg.Net.Rest;

[SuppressMessage(
    "Trimming",
    "IL2026:Members annotated with \'RequiresUnreferencedCodeAttribute\' require dynamic access otherwise can break functionality when trimming application code")]
[SuppressMessage(
    "AOT",
    "IL3050:Calling members annotated with \'RequiresDynamicCodeAttribute\' may break functionality when AOT compiling.")]
public sealed partial class RestCatalogClient(HttpClient httpClient)
{
#pragma warning disable 8618
    private string _baseUrl;
#pragma warning restore 8618

    public string BaseUrl
    {
        get => _baseUrl;
        set
        {
            _baseUrl = value;
            if (!string.IsNullOrEmpty(_baseUrl) && !_baseUrl.EndsWith("/"))
                _baseUrl += '/';
        }
    }

    private static JsonSerializerOptions JsonSerializerOptions => JsonSerializerContext.Options;
    private static JsonSerializerContext JsonSerializerContext => SourceGenerationContext.Default;

    public bool ReadResponseAsString { get; set; }

    partial void PrepareRequest(HttpClient client, HttpRequestMessage request, string url);
    partial void PrepareRequest(HttpClient client, HttpRequestMessage request, StringBuilder urlBuilder);
    partial void ProcessResponse(HttpClient client, HttpResponseMessage response);

    private static bool IsStatusCodeError(int statusCode)
    {
        return statusCode is >= 500 and <= 599;
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     List all catalog configuration settings
    /// </summary>
    /// <remarks>
    ///     All REST clients should first call this route to get catalog configuration properties from the server to configure
    ///     the catalog and its HTTP client. Configuration from the server consists of two sets of key/value pairs.
    ///     <br />- defaults -  properties that should be used as default configuration; applied before client configuration
    ///     <br />- overrides - properties that should be used to override client configuration; applied after defaults and
    ///     client configuration
    ///     <br />
    ///     <br />Catalog configuration is constructed by setting the defaults, then client- provided configuration, and
    ///     finally overrides. The final property set is then used to configure the catalog.
    ///     <br />
    ///     <br />For example, a default configuration property might set the size of the client pool, which can be replaced
    ///     with a client-specific setting. An override might be used to set the warehouse location, which is stored on the
    ///     server rather than in client configuration.
    ///     <br />
    ///     <br />Common catalog configuration settings are documented at
    ///     https://iceberg.apache.org/docs/latest/configuration/#catalog-properties
    ///     <br />
    ///     <br />The catalog configuration also holds an optional `endpoints` field that contains information about the
    ///     endpoints supported by the server. If a server does not send the `endpoints` field, a default set of endpoints is
    ///     assumed:
    ///     <br />- GET /namespaces
    ///     <br />- POST /namespaces
    ///     <br />- GET /namespaces/{namespace}
    ///     <br />- DELETE /namespaces/{namespace}
    ///     <br />- POST /namespaces/{namespace}/properties
    ///     <br />- GET /namespaces/{namespace}/tables
    ///     <br />- POST /namespaces/{namespace}/tables
    ///     <br />- GET /namespaces/{namespace}/tables/{table}
    ///     <br />- POST /namespaces/{namespace}/tables/{table}
    ///     <br />- DELETE /namespaces/{namespace}/tables/{table}
    ///     <br />- POST /namespaces/{namespace}/register
    ///     <br />- POST /namespaces/{namespace}/tables/{table}/metrics
    ///     <br />- POST /tables/rename
    ///     <br />- POST /transactions/commit
    /// </remarks>
    /// <param name="warehouse">Warehouse location or identifier to request from the service</param>
    /// <returns>Server specified configuration values.</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<CatalogConfig> GetConfigAsync(
        string? warehouse = null,
        CancellationToken cancellationToken = default)
    {
        using HttpRequestMessage request = new();
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "config"
        urlBuilder.Append("config");
        urlBuilder.Append('?');
        if (warehouse != null)
            urlBuilder.Append(Uri.EscapeDataString("warehouse")).Append('=')
                .Append(Uri.EscapeDataString(ConvertToString(warehouse, CultureInfo.InvariantCulture)))
                .Append('&');
        urlBuilder.Length--;

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<CatalogConfig> objectResponse =
                await ReadObjectResponseAsync<CatalogConfig>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 419)
        {
            const string message =
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.";
            throw await HandleError<IcebergErrorResponse>(response, headers, status, message, cancellationToken)
                .ConfigureAwait(false);
        }

        if (status == 503)
        {
            const string message =
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.";
            throw await HandleError<IcebergErrorResponse>(response, headers, status, message, cancellationToken)
                .ConfigureAwait(false);
        }

        if (IsStatusCodeError(status))
        {
            const string message =
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.";
            throw await HandleError<IcebergErrorResponse>(response, headers, status, message, cancellationToken)
                .ConfigureAwait(false);
        }

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Get a token using an OAuth2 flow (DEPRECATED for REMOVAL)
    /// </summary>
    /// <remarks>
    ///     The `oauth/tokens` endpoint is **DEPRECATED for REMOVAL**. It is _not_ recommended to implement this endpoint,
    ///     unless you are fully aware of the potential security implications.
    ///     <br />All clients are encouraged to explicitly set the configuration property `oauth2-server-uri` to the correct
    ///     OAuth endpoint.
    ///     <br />Deprecated since Iceberg (Java) 1.6.0. The endpoint and related types will be removed from this spec in
    ///     Iceberg (Java) 2.0.
    ///     <br />See [Security improvements in the Iceberg REST specification](https://github.com/apache/iceberg/issues/10537)
    ///     <br />
    ///     <br />Exchange credentials for a token using the OAuth2 client credentials flow or token exchange.
    ///     <br />
    ///     <br />This endpoint is used for three purposes -
    ///     <br />1. To exchange client credentials (client ID and secret) for an access token This uses the client credentials
    ///     flow.
    ///     <br />2. To exchange a client token and an identity token for a more specific access token This uses the token
    ///     exchange flow.
    ///     <br />3. To exchange an access token for one with the same claims and a refreshed expiration period This uses the
    ///     token exchange flow.
    ///     <br />
    ///     <br />For example, a catalog client may be configured with client credentials from the OAuth2 Authorization flow.
    ///     This client would exchange its client ID and secret for an access token using the client credentials request with
    ///     this endpoint (1). Subsequent requests would then use that access token.
    ///     <br />
    ///     <br />Some clients may also handle sessions that have additional user context. These clients would use the token
    ///     exchange flow to exchange a user token (the "subject" token) from the session for a more specific access token for
    ///     that user, using the catalog's access token as the "actor" token (2). The user ID token is the "subject" token and
    ///     can be any token type allowed by the OAuth2 token exchange flow, including a unsecured JWT token with a sub claim.
    ///     This request should use the catalog's bearer token in the "Authorization" header.
    ///     <br />
    ///     <br />Clients may also use the token exchange flow to refresh a token that is about to expire by sending a token
    ///     exchange request (3). The request's "subject" token should be the expiring token. This request should use the
    ///     subject token in the "Authorization" header.
    /// </remarks>
    /// <returns>OAuth2 token response for client credentials or token exchange</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    [Obsolete]
    public async Task<OAuthTokenResponse> GetTokenAsync(
        OAuthTokenRequest body,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(body);

        using HttpRequestMessage request = new();
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        Dictionary<string, string>? dictionary =
            JsonSerializer.Deserialize<Dictionary<string, string>>(json, JsonSerializerOptions);
        FormUrlEncodedContent content = new(dictionary ?? []);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/x-www-form-urlencoded");
        request.Content = content;
        request.Method = new HttpMethod("POST");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "oauth/tokens"
        urlBuilder.Append("oauth/tokens");

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<OAuthTokenResponse> objectResponse =
                await ReadObjectResponseAsync<OAuthTokenResponse>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400 || status == 401 || IsStatusCodeError(status))
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "OAuth2 error response",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     List namespaces, optionally providing a parent namespace to list underneath
    /// </summary>
    /// <remarks>
    ///     List all namespaces at a certain level, optionally starting from a given parent namespace. If table
    ///     accounting.tax.paid.info exists, using 'SELECT NAMESPACE IN accounting' would translate into `GET
    ///     /namespaces?parent=accounting` and must return a namespace, ["accounting", "tax"] only. Using 'SELECT NAMESPACE IN
    ///     accounting.tax' would translate into `GET /namespaces?parent=accounting%1Ftax` and must return a namespace,
    ///     ["accounting", "tax", "paid"]. If `parent` is not provided, all top-level namespaces should be listed.
    /// </remarks>
    /// <param name="pageSize">
    ///     For servers that support pagination, this signals an upper bound of the number of results that a
    ///     client will receive. For servers that do not support pagination, clients may receive results larger than the
    ///     indicated `pageSize`.
    /// </param>
    /// <param name="parent">
    ///     An optional namespace, underneath which to list namespaces. If not provided, all top-level
    ///     namespaces should be listed. For backward compatibility, empty string is treated as absent for now. If parent is a
    ///     multipart namespace, the parts must be separated by the namespace separator as indicated via the /config override
    ///     `namespace-separator`, which defaults to the unit separator `0x1F` byte (url encoded `%1F`). To be compatible with
    ///     older clients, servers must use both the advertised separator and `0x1F` as valid separators when decoding
    ///     namespaces. The `namespace-separator` should be provided in a url encoded form.
    /// </param>
    /// <returns>A list of namespaces</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<ListNamespacesResponse> ListNamespacesAsync(
        string? pageToken = null,
        int? pageSize = null,
        string? parent = null,
        CancellationToken cancellationToken = default)
    {
        using HttpRequestMessage request = new();
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces"
        urlBuilder.Append("namespaces");
        urlBuilder.Append('?');
        if (pageToken != null)
            urlBuilder.Append(Uri.EscapeDataString("pageToken")).Append('=')
                .Append(Uri.EscapeDataString(ConvertToString(pageToken, CultureInfo.InvariantCulture)))
                .Append('&');
        if (pageSize != null)
            urlBuilder.Append(Uri.EscapeDataString("pageSize")).Append('=')
                .Append(Uri.EscapeDataString(ConvertToString(pageSize, CultureInfo.InvariantCulture)))
                .Append('&');
        if (parent != null)
            urlBuilder.Append(Uri.EscapeDataString("parent")).Append('=')
                .Append(Uri.EscapeDataString(ConvertToString(parent, CultureInfo.InvariantCulture)))
                .Append('&');
        urlBuilder.Length--;

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<ListNamespacesResponse> objectResponse =
                await ReadObjectResponseAsync<ListNamespacesResponse>(
                    response,
                    headers,
                    cancellationToken).ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - Namespace provided in the `parent` query parameter is not found.",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken).ConfigureAwait(false);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Create a namespace
    /// </summary>
    /// <remarks>
    ///     Create a namespace, with an optional set of properties. The server might also add properties, such as
    ///     `last_modified_time` etc.
    /// </remarks>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>
    ///     Represents a successful call to create a namespace. Returns the namespace created, as well as any properties
    ///     that were stored for the namespace, including those the server might have added. Implementations are not required
    ///     to support namespace properties.
    /// </returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<CreateNamespaceResponse> CreateNamespaceAsync(
        CreateNamespaceRequest body,
        Guid? idempotencyKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(body);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces"
        urlBuilder.Append("namespaces");

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<CreateNamespaceResponse> objectResponse =
                await ReadObjectResponseAsync<CreateNamespaceResponse>(
                    response,
                    headers,
                    cancellationToken).ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 406)
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Not Acceptable / Unsupported Operation. The server does not support this operation.",
                cancellationToken);

        if (status == 409)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Conflict - The namespace already exists",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Load the metadata properties for a namespace
    /// </summary>
    /// <remarks>
    ///     Return all stored metadata properties for a given namespace
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <returns>
    ///     Returns a namespace, as well as any properties stored on the namespace if namespace properties are supported
    ///     by the server.
    /// </returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<GetNamespaceResponse> LoadNamespaceMetadataAsync(
        string @namespace,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        using HttpRequestMessage request = new();
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<GetNamespaceResponse> objectResponse =
                await ReadObjectResponseAsync<GetNamespaceResponse>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - Namespace not found",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Check if a namespace exists
    /// </summary>
    /// <remarks>
    ///     Check if a namespace exists. The response does not contain a body.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <returns>Success, no content</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task NamespaceExistsAsync(string @namespace, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        using HttpRequestMessage request = new();
        request.Method = new HttpMethod("HEAD");

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 204)
        {
        }
        else if (status == 400)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);
        }
        else if (status == 401)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);
        }
        else if (status == 403)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);
        }
        else if (status == 404)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - Namespace not found",
                cancellationToken);
        }
        else if (status == 419)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);
        }
        else if (status == 503)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);
        }
        else if (IsStatusCodeError(status))
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);
        }
        else
        {
            var responseData = response.Content == null
                ? null
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException(
                "The HTTP status code of the response was not expected (" + status + ").",
                status,
                responseData,
                headers,
                null);
        }
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Drop a namespace from the catalog. Namespace must be empty.
    /// </summary>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>Success, no content</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task DropNamespaceAsync(
        string @namespace,
        Guid? idempotencyKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        request.Method = new HttpMethod("DELETE");

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 204)
        {
        }
        else if (status == 400)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);
        }
        else if (status == 401)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);
        }
        else if (status == 403)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);
        }
        else if (status == 404)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - Namespace to delete does not exist.",
                cancellationToken);
        }
        else if (status == 409)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Empty - Namespace to delete is not empty.",
                cancellationToken);
        }
        else if (status == 419)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);
        }
        else if (status == 503)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);
        }
        else if (IsStatusCodeError(status))
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);
        }
        else
        {
            var responseData = response.Content == null
                ? null
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException(
                "The HTTP status code of the response was not expected (" + status + ").",
                status,
                responseData,
                headers,
                null);
        }
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Set or remove properties on a namespace
    /// </summary>
    /// <remarks>
    ///     Set and/or remove properties on a namespace. The request body specifies a list of properties to remove and a map of
    ///     key value pairs to update.
    ///     <br />Properties that are not in the request are not modified or removed by this call.
    ///     <br />Server implementations are not required to support namespace properties.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>JSON data response for a synchronous update properties request.</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<UpdateNamespacePropertiesResponse> UpdatePropertiesAsync(
        UpdateNamespacePropertiesRequest body,
        string @namespace,
        Guid? idempotencyKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(body);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/properties"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/properties");

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<UpdateNamespacePropertiesResponse> objectResponse =
                await ReadObjectResponseAsync<UpdateNamespacePropertiesResponse>(
                    response,
                    headers,
                    cancellationToken).ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - Namespace not found",
                cancellationToken);

        if (status == 406)
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Not Acceptable / Unsupported Operation. The server does not support this operation.",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 422)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unprocessable Entity - A property key was included in both `removals` and `updates`",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     List all table identifiers underneath a given namespace
    /// </summary>
    /// <remarks>
    ///     Return all table identifiers under this namespace
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="pageSize">
    ///     For servers that support pagination, this signals an upper bound of the number of results that a
    ///     client will receive. For servers that do not support pagination, clients may receive results larger than the
    ///     indicated `pageSize`.
    /// </param>
    /// <returns>A list of table identifiers</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<ListTablesResponse> ListTablesAsync(
        string @namespace,
        string? pageToken = null,
        int? pageSize = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        using HttpRequestMessage request = new();
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/tables"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tables");
        urlBuilder.Append('?');
        if (pageToken != null)
            urlBuilder.Append(Uri.EscapeDataString("pageToken")).Append('=')
                .Append(Uri.EscapeDataString(ConvertToString(pageToken, CultureInfo.InvariantCulture)))
                .Append('&');
        if (pageSize != null)
            urlBuilder.Append(Uri.EscapeDataString("pageSize")).Append('=')
                .Append(Uri.EscapeDataString(ConvertToString(pageSize, CultureInfo.InvariantCulture)))
                .Append('&');
        urlBuilder.Length--;

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<ListTablesResponse> objectResponse =
                await ReadObjectResponseAsync<ListTablesResponse>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - The namespace specified does not exist",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Create a table in the given namespace
    /// </summary>
    /// <remarks>
    ///     Create a table or start a create transaction, like atomic CTAS.
    ///     <br />
    ///     <br />If `stage-create` is false, the table is created immediately.
    ///     <br />
    ///     <br />If `stage-create` is true, the table is not created, but table metadata is initialized and returned. The
    ///     service should prepare as needed for a commit to the table commit endpoint to complete the create transaction. The
    ///     client uses the returned metadata to begin a transaction. To commit the transaction, the client sends all create
    ///     and subsequent changes to the table commit route. Changes from the table create operation include changes like
    ///     AddSchemaUpdate and SetCurrentSchemaUpdate that set the initial table state.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="xIcebergAccessDelegation">
    ///     Optional signal to the server that the client supports delegated access via a comma-separated list of access
    ///     mechanisms.  The server may choose to supply access via any or none of the requested mechanisms.
    ///     <br />
    ///     <br />Specific properties and handling for `vended-credentials` is documented in the `LoadTableResult` schema
    ///     section of this spec document.
    ///     <br />
    ///     <br />The protocol and specification for `remote-signing` is documented in the `s3-signer-open-api.yaml` OpenApi
    ///     spec in the `aws` module.
    /// </param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>Table metadata result after creating a table</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<LoadTableResult> CreateTableAsync(
        CreateTableRequest body,
        string @namespace,
        XIcebergAccessDelegation? xIcebergAccessDelegation = XIcebergAccessDelegation.VendedCredentials,
        Guid? idempotencyKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(body);

        using HttpRequestMessage request = new();
        if (xIcebergAccessDelegation != null)
            request.Headers.TryAddWithoutValidation(
                "X-Iceberg-Access-Delegation",
                ConvertToString(xIcebergAccessDelegation, CultureInfo.InvariantCulture));

        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        var aaa = JsonSerializer.Serialize(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/tables"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tables");

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<LoadTableResult> objectResponse =
                await ReadObjectResponseAsync<LoadTableResult>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - The namespace specified does not exist",
                cancellationToken);

        if (status == 409)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Conflict - The table already exists",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Submit a scan for planning
    /// </summary>
    /// <remarks>
    ///     Submits a scan for server-side planning.
    ///     <br />
    ///     <br />Point-in-time scans are planned by passing snapshot-id to identify the table snapshot to scan. Incremental
    ///     scans are planned by passing both start-snapshot-id and end-snapshot-id. Requests that include both point in time
    ///     config properties and incremental config properties are invalid. If the request does not include either incremental
    ///     or point-in-time config properties, scan planning should produce a point-in-time scan of the latest snapshot in the
    ///     table's main branch.
    ///     <br />
    ///     <br />Responses must include a valid status listed below. A "cancelled" status is considered invalid for this
    ///     endpoint.
    ///     <br />- When "completed" the planning operation has produced plan tasks and
    ///     <br />  file scan tasks that must be returned in the response (not fetched
    ///     <br />  later by calling fetchPlanningResult)
    ///     <br />
    ///     <br />- When "submitted" the response must include a plan-id used to poll
    ///     <br />  fetchPlanningResult to fetch the planning result when it is ready
    ///     <br />
    ///     <br />- When "failed" the response must be a valid error response
    ///     <br />The response for a "completed" planning operation includes two types of tasks (file scan tasks and plan
    ///     tasks) and both may be included in the response. Tasks must not be included for any other response status.
    ///     <br />
    ///     <br />Responses that include a plan-id indicate that the service is holding state or performing work for the
    ///     client.
    ///     <br />
    ///     <br />- Clients should use the plan-id to fetch results from
    ///     <br />  fetchPlanningResult when the response status is "submitted"
    ///     <br />
    ///     <br />- Clients should inform the service if planning results are no longer
    ///     <br />  needed by calling cancelPlanning. Cancellation is not necessary after
    ///     <br />  fetchScanTasks has been used to fetch scan tasks for each plan task.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="table">A table name</param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>Result of submitting a table scan to plan</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<CompletedPlanningWithIdResult> PlanTableScanAsync(
        string @namespace,
        string table,
        Guid? idempotencyKey = null,
        PlanTableScanRequest? body = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(table);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/tables/{table}/plan"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tables/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(table, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/plan");

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<CompletedPlanningWithIdResult> objectResponse =
                await ReadObjectResponseAsync<CompletedPlanningWithIdResult>(
                    response,
                    headers,
                    cancellationToken).ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - NoSuchTableException, the table does not exist - NoSuchNamespaceException, the namespace does not exist",
                cancellationToken);

        if (status == 406)
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Not Acceptable / Unsupported Operation. The server does not support this operation.",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Fetches the result of scan planning for a plan-id
    /// </summary>
    /// <remarks>
    ///     Fetches the result of scan planning for a plan-id.
    ///     <br />
    ///     <br />Responses must include a valid status
    ///     <br />- When "completed" the planning operation has produced plan-tasks and
    ///     <br />  file-scan-tasks that must be returned in the response
    ///     <br />
    ///     <br />- When "submitted" the planning operation has not completed; the client
    ///     <br />  should wait to call this endpoint again to fetch a completed response
    ///     <br />
    ///     <br />- When "failed" the response must be a valid error response
    ///     <br />- When "cancelled" the plan-id is invalid and should be discarded
    ///     <br />
    ///     <br />The response for a "completed" planning operation includes two types of tasks (file scan tasks and plan
    ///     tasks) and both may be included in the response. Tasks must not be included for any other response status.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="table">A table name</param>
    /// <param name="planId">ID used to track a planning request</param>
    /// <returns>Result of fetching a submitted scan planning operation</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<CompletedPlanningResult> FetchPlanningResultAsync(
        string @namespace,
        string table,
        string planId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(table);

        ArgumentNullException.ThrowIfNull(planId);

        using HttpRequestMessage request = new();
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/tables/{table}/plan/{plan-id}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tables/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(table, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/plan/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(planId, CultureInfo.InvariantCulture)));

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<CompletedPlanningResult> objectResponse =
                await ReadObjectResponseAsync<CompletedPlanningResult>(
                    response,
                    headers,
                    cancellationToken).ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - NoSuchPlanIdException, the plan-id does not exist - NoSuchTableException, the table does not exist - NoSuchNamespaceException, the namespace does not exist",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Cancels scan planning for a plan-id
    /// </summary>
    /// <remarks>
    ///     Cancels scan planning for a plan-id.
    ///     <br />
    ///     <br />This notifies the service that it can release resources held for the scan. Clients should cancel scans that
    ///     are no longer needed, either while the plan-id returns a "submitted" status or while there are remaining plan tasks
    ///     that have not been fetched.
    ///     <br />
    ///     <br />Cancellation is not necessary when
    ///     <br />- Scan tasks for each plan task have been fetched using fetchScanTasks
    ///     <br />- A plan-id has produced a "failed" or "cancelled" status from
    ///     <br />  planTableScan or fetchPlanningResult
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="table">A table name</param>
    /// <param name="planId">ID used to track a planning request</param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>Success, no content</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task CancelPlanningAsync(
        string @namespace,
        string table,
        string planId,
        Guid? idempotencyKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(table);

        ArgumentNullException.ThrowIfNull(planId);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        request.Method = new HttpMethod("DELETE");

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/tables/{table}/plan/{plan-id}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tables/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(table, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/plan/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(planId, CultureInfo.InvariantCulture)));

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 204)
        {
        }
        else if (status == 400)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);
        }
        else if (status == 401)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);
        }
        else if (status == 403)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);
        }
        else if (status == 404)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - NoSuchTableException, the table does not exist - NoSuchNamespaceException, the namespace does not exist",
                cancellationToken);
        }
        else if (status == 419)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);
        }
        else if (status == 503)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);
        }
        else if (IsStatusCodeError(status))
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);
        }
        else
        {
            var responseData = response.Content == null
                ? null
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException(
                "The HTTP status code of the response was not expected (" + status + ").",
                status,
                responseData,
                headers,
                null);
        }
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Fetches result tasks for a plan task
    /// </summary>
    /// <remarks>
    ///     Fetches result tasks for a plan task.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="table">A table name</param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>Result of retrieving additional plan tasks and file scan tasks.</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<FetchScanTasksResult> FetchScanTasksAsync(
        string @namespace,
        string table,
        Guid? idempotencyKey = null,
        FetchScanTasksRequest? body = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(table);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/tables/{table}/tasks"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tables/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(table, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tasks");

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<FetchScanTasksResult> objectResponse =
                await ReadObjectResponseAsync<FetchScanTasksResult>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - NoSuchPlanTaskException, the plan-task does not exist - NoSuchTableException, the table does not exist - NoSuchNamespaceException, the namespace does not exist",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Register a table in the given namespace using given metadata file location
    /// </summary>
    /// <remarks>
    ///     Register a table using given metadata file location.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>Table metadata result when loading a table</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<LoadTableResult> RegisterTableAsync(
        RegisterTableRequest body,
        string @namespace,
        Guid? idempotencyKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(body);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/register"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/register");

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<LoadTableResult> objectResponse =
                await ReadObjectResponseAsync<LoadTableResult>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - The namespace specified does not exist",
                cancellationToken);

        if (status == 409)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Conflict - The table already exists",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Load a table from the catalog
    /// </summary>
    /// <remarks>
    ///     Load a table from the catalog.
    ///     <br />
    ///     <br />The response contains both configuration and table metadata. The configuration, if non-empty is used as
    ///     additional configuration for the table that overrides catalog configuration. For example, this configuration may
    ///     change the FileIO implementation to be used for the table.
    ///     <br />
    ///     <br />The response also contains the table's full metadata, matching the table metadata JSON file.
    ///     <br />
    ///     <br />The catalog configuration may contain credentials that should be used for subsequent requests for the table.
    ///     The configuration key "token" is used to pass an access token to be used as a bearer token for table requests.
    ///     Otherwise, a token may be passed using a RFC 8693 token type as a configuration key. For example,
    ///     "urn:ietf:params:oauth:token-type:jwt=&lt;JWT-token&gt;".
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="table">A table name</param>
    /// <param name="xIcebergAccessDelegation">
    ///     Optional signal to the server that the client supports delegated access via a comma-separated list of access
    ///     mechanisms.  The server may choose to supply access via any or none of the requested mechanisms.
    ///     <br />
    ///     <br />Specific properties and handling for `vended-credentials` is documented in the `LoadTableResult` schema
    ///     section of this spec document.
    ///     <br />
    ///     <br />The protocol and specification for `remote-signing` is documented in the `s3-signer-open-api.yaml` OpenApi
    ///     spec in the `aws` module.
    /// </param>
    /// <param name="ifNoneMatch">
    ///     An optional header that allows the server to return 304 (Not Modified) if the metadata is
    ///     current. The content is the value of the ETag received in a CreateTableResponse or LoadTableResponse.
    /// </param>
    /// <param name="snapshots">
    ///     The snapshots to return in the body of the metadata. Setting the value to `all` would return the full set of
    ///     snapshots currently valid for the table. Setting the value to `refs` would load all snapshots referenced by
    ///     branches or tags.
    ///     <br />Default if no param is provided is `all`.
    /// </param>
    /// <returns>Table metadata result when loading a table</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<LoadTableResult> LoadTableAsync(
        string @namespace,
        string table,
        XIcebergAccessDelegation? xIcebergAccessDelegation = XIcebergAccessDelegation.VendedCredentials,
        string? ifNoneMatch = null,
        Snapshots? snapshots = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(table);

        using HttpRequestMessage request = new();
        if (xIcebergAccessDelegation != null)
            request.Headers.TryAddWithoutValidation(
                "X-Iceberg-Access-Delegation",
                ConvertToString(xIcebergAccessDelegation, CultureInfo.InvariantCulture));

        if (ifNoneMatch != null)
            request.Headers.TryAddWithoutValidation(
                "If-None-Match",
                ConvertToString(ifNoneMatch, CultureInfo.InvariantCulture));
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/tables/{table}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tables/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(table, CultureInfo.InvariantCulture)));
        urlBuilder.Append('?');
        if (snapshots != null)
            urlBuilder.Append(Uri.EscapeDataString("snapshots")).Append('=')
                .Append(Uri.EscapeDataString(ConvertToString(snapshots, CultureInfo.InvariantCulture)))
                .Append('&');
        urlBuilder.Length--;

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<LoadTableResult> objectResponse =
                await ReadObjectResponseAsync<LoadTableResult>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 304)
        {
            var responseText = response.Content == null
                ? string.Empty
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException(
                "Not Modified - Based on the content of the \'If-None-Match\' header the table metadata has not changed since.",
                status,
                responseText,
                headers,
                null);
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - NoSuchTableException, table to load does not exist",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Commit updates to a table
    /// </summary>
    /// <remarks>
    ///     Commit updates to a table.
    ///     <br />
    ///     <br />Commits have two parts, requirements and updates. Requirements are assertions that will be validated before
    ///     attempting to make and commit changes. For example, `assert-ref-snapshot-id` will check that a named ref's snapshot
    ///     ID has a certain value. Server implementations are required to fail with a 400 status code if any unknown updates
    ///     or requirements are received.
    ///     <br />
    ///     <br />Updates are changes to make to table metadata. For example, after asserting that the current main ref is at
    ///     the expected snapshot, a commit may add a new child snapshot and set the ref to the new snapshot id.
    ///     <br />
    ///     <br />Create table transactions that are started by createTable with `stage-create` set to true are committed using
    ///     this route. Transactions should include all changes to the table, including table initialization, like
    ///     AddSchemaUpdate and SetCurrentSchemaUpdate. The `assert-create` requirement is used to ensure that the table was
    ///     not created concurrently.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="table">A table name</param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>
    ///     Response used when a table is successfully updated.
    ///     <br />The table metadata JSON is returned in the metadata field. The corresponding file location of table metadata
    ///     must be returned in the metadata-location field. Clients can check whether metadata has changed by comparing
    ///     metadata locations.
    /// </returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<CommitTableResponse> UpdateTableAsync(
        CommitTableRequest body,
        string @namespace,
        string table,
        Guid? idempotencyKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(table);

        ArgumentNullException.ThrowIfNull(body);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        var txt = JsonSerializer.Serialize(body, JsonSerializerOptions);
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/tables/{table}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tables/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(table, CultureInfo.InvariantCulture)));

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<CommitTableResponse> objectResponse =
                await ReadObjectResponseAsync<CommitTableResponse>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - NoSuchTableException, table to load does not exist",
                cancellationToken);

        if (status == 409)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Conflict - CommitFailedException, one or more requirements failed. The client may retry.",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 500)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "An unknown server-side problem occurred; the commit state is unknown.",
                cancellationToken);

        if (status == 502)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A gateway or proxy received an invalid response from the upstream server; the commit state is unknown.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (status == 504)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side gateway timeout occurred; the commit state is unknown.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable on the client.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Drop a table from the catalog
    /// </summary>
    /// <remarks>
    ///     Remove a table from the catalog
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="table">A table name</param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <param name="purgeRequested">Whether the user requested to purge the underlying table's data and metadata</param>
    /// <returns>Success, no content</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task DropTableAsync(
        string @namespace,
        string table,
        Guid? idempotencyKey = null,
        bool? purgeRequested = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(table);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        request.Method = new HttpMethod("DELETE");

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/tables/{table}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tables/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(table, CultureInfo.InvariantCulture)));
        urlBuilder.Append('?');
        if (purgeRequested != null)
            urlBuilder.Append(Uri.EscapeDataString("purgeRequested")).Append('=')
                .Append(Uri.EscapeDataString(ConvertToString(purgeRequested, CultureInfo.InvariantCulture)))
                .Append('&');
        urlBuilder.Length--;

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 204)
        {
        }
        else if (status == 400)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);
        }
        else if (status == 401)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);
        }
        else if (status == 403)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);
        }
        else if (status == 404)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - NoSuchTableException, Table to drop does not exist",
                cancellationToken);
        }
        else if (status == 419)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);
        }
        else if (status == 503)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);
        }
        else if (IsStatusCodeError(status))
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);
        }
        else
        {
            var responseData = response.Content == null
                ? null
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException(
                "The HTTP status code of the response was not expected (" + status + ").",
                status,
                responseData,
                headers,
                null);
        }
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Check if a table exists
    /// </summary>
    /// <remarks>
    ///     Check if a table exists within a given namespace. The response does not contain a body.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="table">A table name</param>
    /// <returns>Success, no content</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task TableExistsAsync(
        string @namespace,
        string table,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(table);

        using HttpRequestMessage request = new();
        request.Method = new HttpMethod("HEAD");

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/tables/{table}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tables/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(table, CultureInfo.InvariantCulture)));

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 204)
        {
        }
        else if (status == 400)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);
        }
        else if (status == 401)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);
        }
        else if (status == 403)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);
        }
        else if (status == 404)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - NoSuchTableException, Table not found",
                cancellationToken);
        }
        else if (status == 419)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);
        }
        else if (status == 503)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);
        }
        else if (IsStatusCodeError(status))
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);
        }
        else
        {
            var responseData = response.Content == null
                ? null
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException(
                "The HTTP status code of the response was not expected (" + status + ").",
                status,
                responseData,
                headers,
                null);
        }
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Load vended credentials for a table from the catalog
    /// </summary>
    /// <remarks>
    ///     Load vended credentials for a table from the catalog.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="table">A table name</param>
    /// <param name="planId">The plan ID that has been used for server-side scan planning</param>
    /// <returns>Table credentials result when loading credentials for a table</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<LoadCredentialsResponse> LoadCredentialsAsync(
        string @namespace,
        string table,
        string? planId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(table);

        using HttpRequestMessage request = new();
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/tables/{table}/credentials"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tables/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(table, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/credentials");
        urlBuilder.Append('?');
        if (planId != null)
            urlBuilder.Append(Uri.EscapeDataString("planId")).Append('=')
                .Append(Uri.EscapeDataString(ConvertToString(planId, CultureInfo.InvariantCulture)))
                .Append('&');
        urlBuilder.Length--;

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<LoadCredentialsResponse> objectResponse =
                await ReadObjectResponseAsync<LoadCredentialsResponse>(
                    response,
                    headers,
                    cancellationToken).ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - NoSuchTableException, table to load credentials for does not exist",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Rename a table from its current name to a new name
    /// </summary>
    /// <remarks>
    ///     Rename a table from one identifier to another. It's valid to move a table across namespaces, but the server
    ///     implementation is not required to support it.
    /// </remarks>
    /// <param name="body">Current table identifier to rename and new table identifier to rename to</param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>Success, no content</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task RenameTableAsync(
        RenameTableRequest body,
        Guid? idempotencyKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(body);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "tables/rename"
        urlBuilder.Append("tables/rename");

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 204)
        {
        }
        else if (status == 400)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);
        }
        else if (status == 401)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);
        }
        else if (status == 403)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);
        }
        else if (status == 404)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - NoSuchTableException, Table to rename does not exist - NoSuchNamespaceException, The target namespace of the new table identifier does not exist",
                cancellationToken);
        }
        else if (status == 406)
        {
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Not Acceptable / Unsupported Operation. The server does not support this operation.",
                cancellationToken);
        }
        else if (status == 409)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Conflict - The target identifier to rename to already exists as a table or view",
                cancellationToken);
        }
        else if (status == 419)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);
        }
        else if (status == 503)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);
        }
        else if (IsStatusCodeError(status))
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);
        }
        else
        {
            var responseData = response.Content == null
                ? null
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException(
                "The HTTP status code of the response was not expected (" + status + ").",
                status,
                responseData,
                headers,
                null);
        }
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Send a metrics report to this endpoint to be processed by the backend
    /// </summary>
    /// <param name="body">The request containing the metrics report to be sent</param>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="table">A table name</param>
    /// <returns>Success, no content</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task ReportMetricsAsync(
        ReportMetricsRequest body,
        string @namespace,
        string table,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(table);

        ArgumentNullException.ThrowIfNull(body);

        using HttpRequestMessage request = new();
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/tables/{table}/metrics"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/tables/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(table, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/metrics");

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 204)
        {
        }
        else if (status == 400)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);
        }
        else if (status == 401)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);
        }
        else if (status == 403)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);
        }
        else if (status == 404)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - NoSuchTableException, table to load does not exist",
                cancellationToken);
        }
        else if (status == 419)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);
        }
        else if (status == 503)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);
        }
        else if (IsStatusCodeError(status))
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);
        }
        else
        {
            var responseData = response.Content == null
                ? null
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException(
                "The HTTP status code of the response was not expected (" + status + ").",
                status,
                responseData,
                headers,
                null);
        }
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Commit updates to multiple tables in an atomic operation
    /// </summary>
    /// <param name="body">
    ///     Commit updates to multiple tables in an atomic operation
    ///     <br />
    ///     <br />A commit for a single table consists of a table identifier with requirements and updates. Requirements are
    ///     assertions that will be validated before attempting to make and commit changes. For example,
    ///     `assert-ref-snapshot-id` will check that a named ref's snapshot ID has a certain value. Server implementations are
    ///     required to fail with a 400 status code if any unknown updates or requirements are received.
    ///     <br />Updates are changes to make to table metadata. For example, after asserting that the current main ref is at
    ///     the expected snapshot, a commit may add a new child snapshot and set the ref to the new snapshot id.
    /// </param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>Success, no content</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task CommitTransactionAsync(
        CommitTransactionRequest body,
        Guid? idempotencyKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(body);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "transactions/commit"
        urlBuilder.Append("transactions/commit");

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 204)
        {
        }
        else if (status == 400)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);
        }
        else if (status == 401)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);
        }
        else if (status == 403)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);
        }
        else if (status == 404)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Not Found - NoSuchTableException, table to load does not exist",
                cancellationToken);
        }
        else if (status == 409)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Conflict - CommitFailedException, one or more requirements failed. The client may retry.",
                cancellationToken);
        }
        else if (status == 419)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);
        }
        else if (status == 500)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "An unknown server-side problem occurred; the commit state is unknown.",
                cancellationToken);
        }
        else if (status == 502)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A gateway or proxy received an invalid response from the upstream server; the commit state is unknown.",
                cancellationToken);
        }
        else if (status == 503)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);
        }
        else if (status == 504)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side gateway timeout occurred; the commit state is unknown.",
                cancellationToken);
        }
        else if (IsStatusCodeError(status))
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable on the client.",
                cancellationToken);
        }
        else
        {
            var responseData = response.Content == null
                ? null
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException(
                "The HTTP status code of the response was not expected (" + status + ").",
                status,
                responseData,
                headers,
                null);
        }
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     List all view identifiers underneath a given namespace
    /// </summary>
    /// <remarks>
    ///     Return all view identifiers under this namespace
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="pageSize">
    ///     For servers that support pagination, this signals an upper bound of the number of results that a
    ///     client will receive. For servers that do not support pagination, clients may receive results larger than the
    ///     indicated `pageSize`.
    /// </param>
    /// <returns>A list of table identifiers</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<ListTablesResponse> ListViewsAsync(
        string @namespace,
        string? pageToken = null,
        int? pageSize = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        using HttpRequestMessage request = new();
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/views"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/views");
        urlBuilder.Append('?');
        if (pageToken != null)
            urlBuilder.Append(Uri.EscapeDataString("pageToken")).Append('=')
                .Append(Uri.EscapeDataString(ConvertToString(pageToken, CultureInfo.InvariantCulture)))
                .Append('&');
        if (pageSize != null)
            urlBuilder.Append(Uri.EscapeDataString("pageSize")).Append('=')
                .Append(Uri.EscapeDataString(ConvertToString(pageSize, CultureInfo.InvariantCulture)))
                .Append('&');
        urlBuilder.Length--;

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<ListTablesResponse> objectResponse =
                await ReadObjectResponseAsync<ListTablesResponse>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Not Found - The namespace specified does not exist",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Create a view in the given namespace
    /// </summary>
    /// <remarks>
    ///     Create a view in the given namespace.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <returns>View metadata result when loading a view</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<LoadViewResult> CreateViewAsync(
        CreateViewRequest body,
        string @namespace,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(body);

        using HttpRequestMessage request = new();
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/views"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/views");

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<LoadViewResult> objectResponse =
                await ReadObjectResponseAsync<LoadViewResult>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
        {
            const string message =
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.";
            throw await HandleError<IcebergErrorResponse>(response, headers, status, message, cancellationToken)
                .ConfigureAwait(false);
        }

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Not Found - The namespace specified does not exist",
                cancellationToken);

        if (status == 409)
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Conflict - The view already exists",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Load a view from the catalog
    /// </summary>
    /// <remarks>
    ///     Load a view from the catalog.
    ///     <br />
    ///     <br />The response contains both configuration and view metadata. The configuration, if non-empty is used as
    ///     additional configuration for the view that overrides catalog configuration.
    ///     <br />
    ///     <br />The response also contains the view's full metadata, matching the view metadata JSON file.
    ///     <br />
    ///     <br />The catalog configuration may contain credentials that should be used for subsequent requests for the view.
    ///     The configuration key "token" is used to pass an access token to be used as a bearer token for view requests.
    ///     Otherwise, a token may be passed using a RFC 8693 token type as a configuration key. For example,
    ///     "urn:ietf:params:oauth:token-type:jwt=&lt;JWT-token&gt;".
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="view">A view name</param>
    /// <returns>View metadata result when loading a view</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<LoadViewResult> LoadViewAsync(
        string @namespace,
        string view,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(view);

        using HttpRequestMessage request = new();
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/views/{view}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/views/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(view, CultureInfo.InvariantCulture)));

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<LoadViewResult> objectResponse =
                await ReadObjectResponseAsync<LoadViewResult>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Not Found - NoSuchViewException, view to load does not exist",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Replace a view
    /// </summary>
    /// <remarks>
    ///     Commit updates to a view.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="view">A view name</param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>View metadata result when loading a view</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task<LoadViewResult> ReplaceViewAsync(
        CommitViewRequest body,
        string @namespace,
        string view,
        Guid? idempotencyKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(view);

        ArgumentNullException.ThrowIfNull(body);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/views/{view}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/views/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(view, CultureInfo.InvariantCulture)));

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 200)
        {
            ObjectResponseResult<LoadViewResult> objectResponse =
                await ReadObjectResponseAsync<LoadViewResult>(response, headers, cancellationToken)
                    .ConfigureAwait(false);
            if (objectResponse.Object == null)
                throw new IcebergRestException(
                    "Response was null which was not expected.",
                    status,
                    objectResponse.Text,
                    headers,
                    null);
            return objectResponse.Object;
        }

        if (status == 400)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);

        if (status == 401)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);

        if (status == 403)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);

        if (status == 404)
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Not Found - NoSuchViewException, view to load does not exist",
                cancellationToken);

        if (status == 409)
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Conflict - CommitFailedException. The client may retry.",
                cancellationToken);

        if (status == 419)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);

        if (status == 500)
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "An unknown server-side problem occurred; the commit state is unknown.",
                cancellationToken);

        if (status == 502)
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "A gateway or proxy received an invalid response from the upstream server; the commit state is unknown.",
                cancellationToken);

        if (status == 503)
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);

        if (status == 504)
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "A server-side gateway timeout occurred; the commit state is unknown.",
                cancellationToken);

        if (IsStatusCodeError(status))
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable on the client.",
                cancellationToken);

        var responseData = response.Content == null
            ? null
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new IcebergRestException(
            "The HTTP status code of the response was not expected (" + status + ").",
            status,
            responseData,
            headers,
            null);
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Drop a view from the catalog
    /// </summary>
    /// <remarks>
    ///     Remove a view from the catalog
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="view">A view name</param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>Success, no content</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task DropViewAsync(
        string @namespace,
        string view,
        Guid? idempotencyKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(view);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        request.Method = new HttpMethod("DELETE");

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/views/{view}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/views/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(view, CultureInfo.InvariantCulture)));

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 204)
        {
        }
        else if (status == 400)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server\'s middleware.",
                cancellationToken);
        }
        else if (status == 401)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);
        }
        else if (status == 403)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);
        }
        else if (status == 404)
        {
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Not Found - NoSuchViewException, view to drop does not exist",
                cancellationToken);
        }
        else if (status == 419)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);
        }
        else if (status == 503)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);
        }
        else if (IsStatusCodeError(status))
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);
        }
        else
        {
            var responseData = response.Content == null
                ? null
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException(
                "The HTTP status code of the response was not expected (" + status + ").",
                status,
                responseData,
                headers,
                null);
        }
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Check if a view exists
    /// </summary>
    /// <remarks>
    ///     Check if a view exists within a given namespace. This request does not return a response body.
    /// </remarks>
    /// <param name="namespace">
    ///     A namespace identifier as a single string. Multipart namespace parts must be separated by the
    ///     namespace separator as indicated via the /config override `namespace-separator`, which defaults to the unit
    ///     separator `0x1F` byte (url encoded `%1F`). To be compatible with older clients, servers must use both the
    ///     advertised separator and `0x1F` as valid separators when decoding namespaces. The `namespace-separator` should be
    ///     provided in a url encoded form.
    /// </param>
    /// <param name="view">A view name</param>
    /// <returns>Success, no content</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task ViewExistsAsync(
        string @namespace,
        string view,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);

        ArgumentNullException.ThrowIfNull(view);

        using HttpRequestMessage request = new();
        request.Method = new HttpMethod("HEAD");

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "namespaces/{namespace}/views/{view}"
        urlBuilder.Append("namespaces/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(@namespace, CultureInfo.InvariantCulture)));
        urlBuilder.Append("/views/");
        urlBuilder.Append(Uri.EscapeDataString(ConvertToString(view, CultureInfo.InvariantCulture)));

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);

        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 204)
        {
        }
        else if (status == 400)
        {
            var responseText = response.Content == null
                ? string.Empty
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException("Bad Request", status, responseText, headers, null);
        }
        else if (status == 401)
        {
            var responseText = response.Content == null
                ? string.Empty
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException("Unauthorized", status, responseText, headers, null);
        }
        else if (status == 404)
        {
            var responseText = response.Content == null
                ? string.Empty
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException("Not Found", status, responseText, headers, null);
        }
        else if (status == 419)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);
        }
        else if (status == 503)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);
        }
        else if (IsStatusCodeError(status))
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);
        }
        else
        {
            var responseData = response.Content == null
                ? null
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException(
                "The HTTP status code of the response was not expected (" + status + ").",
                status,
                responseData,
                headers,
                null);
        }
    }

    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Rename a view from its current name to a new name
    /// </summary>
    /// <remarks>
    ///     Rename a view from one identifier to another. It's valid to move a view across namespaces, but the server
    ///     implementation is not required to support it.
    /// </remarks>
    /// <param name="body">Current view identifier to rename and new view identifier to rename to</param>
    /// <param name="idempotencyKey">
    ///     Optional client-provided idempotency key for safe request retries.
    ///     <br />
    ///     <br />When present, the server ensures no additional effects for requests that carry the same
    ///     <br />Idempotency-Key. If a prior request with this key has been finalized, the server returns
    ///     <br />an equivalent final response without re-running the operation. The response body may
    ///     <br />reflect a newer state of the catalog than existed at the time of the commit.
    ///     <br />
    ///     <br />Finalization rules:
    ///     <br />- Finalize &amp; replay: 200, 201, 204, and deterministic terminal 4xx (including 409
    ///     <br />  such as AlreadyExists, NamespaceNotEmpty, etc.)
    ///     <br />- Do not finalize (not stored/replayed): 5xx
    ///     <br />
    ///     <br />Key Requirements:
    ///     <br />- Key format: UUIDv7 in string form (RFC 9562).
    ///     <br />- The idempotency key must be globally unique (no reuse across different operations).
    ///     <br />- Catalogs SHOULD NOT expire keys before the end of the advertised token lifetime.
    ///     <br />- If Idempotency-Key is used, clients MUST reuse the same key when retrying the same
    ///     <br />  logical operation and MUST generate a new key for a different operation.
    /// </param>
    /// <returns>Success, no content</returns>
    /// <exception cref="IcebergRestException">A server side error occurred.</exception>
    public async Task RenameViewAsync(
        RenameTableRequest body,
        Guid? idempotencyKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(body);

        using HttpRequestMessage request = new();
        if (idempotencyKey != null)
            request.Headers.TryAddWithoutValidation(
                "Idempotency-Key",
                ConvertToString(idempotencyKey, CultureInfo.InvariantCulture));
        var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonSerializerOptions);
        ByteArrayContent content = new(json);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");

        StringBuilder urlBuilder = new();
        if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder.Append(_baseUrl);
        // Operation Path: "views/rename"
        urlBuilder.Append("views/rename");

        PrepareRequest(httpClient, request, urlBuilder);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        PrepareRequest(httpClient, request, url);

        using HttpResponseMessage response = await httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<string, IEnumerable<string>> headers = new();
        foreach (KeyValuePair<string, IEnumerable<string>> item in response.Headers)
            headers[item.Key] = item.Value;
        if (response.Content != null && response.Content.Headers != null)
            foreach (KeyValuePair<string, IEnumerable<string>> item in response.Content.Headers)
                headers[item.Key] = item.Value;

        ProcessResponse(httpClient, response);

        var status = (int)response.StatusCode;
        if (status == 204)
        {
        }
        else if (status == 400)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server's middleware.",
                cancellationToken);
        }
        else if (status == 401)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Unauthorized. The REST Catalog SHOULD respond with the 401 UnauthorizedResponse when the access token provided is expired, revoked, malformed, or invalid for other reasons. The client MAY request a new access token and retry the request.",
                cancellationToken);
        }
        else if (status == 403)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "Forbidden. Authenticated user does not have the necessary permissions.",
                cancellationToken);
        }
        else if (status == 404)
        {
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Not Found - NoSuchViewException, view to rename does not exist - NoSuchNamespaceException, The target namespace of the new identifier does not exist",
                cancellationToken);
        }
        else if (status == 406)
        {
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Not Acceptable / Unsupported Operation. The server does not support this operation.",
                cancellationToken);
        }
        else if (status == 409)
        {
            throw await HandleError<OAuthError>(
                response,
                headers,
                status,
                "Conflict - The target identifier to rename to already exists as a table or view",
                cancellationToken);
        }
        else if (status == 419)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "This is an optional status response type that the REST Catalog can issue when the token has expired. The client MAY request a new access token and retry the request. 401 UnauthorizedResponse SHOULD be preferred over this response type on token expiry.",
                cancellationToken);
        }
        else if (status == 503)
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "The service is not ready to handle the request, request could have been partially processed.\nThe service may additionally send a Retry-After header to indicate when to retry, a non idempotent request should only be retried by the client when the Retry-After header is present.",
                cancellationToken);
        }
        else if (IsStatusCodeError(status))
        {
            throw await HandleError<IcebergErrorResponse>(
                response,
                headers,
                status,
                "A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes.",
                cancellationToken);
        }
        else
        {
            var responseData = response.Content == null
                ? null
                : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new IcebergRestException(
                "The HTTP status code of the response was not expected (" + status + ").",
                status,
                responseData,
                headers,
                null);
        }
    }

    private async Task<ObjectResponseResult<T>> ReadObjectResponseAsync<T>(
        HttpResponseMessage response,
        IReadOnlyDictionary<string, IEnumerable<string>> headers,
        CancellationToken cancellationToken)
    {
        if (response == null || response.Content == null) return new ObjectResponseResult<T>(default, string.Empty);

        if (ReadResponseAsString)
        {
            var responseText = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                T? typedBody = JsonSerializer.Deserialize<T>(responseText, JsonSerializerOptions);
                return new ObjectResponseResult<T>(typedBody, responseText);
            }
            catch (JsonException exception)
            {
                var message = "Could not deserialize the response body string as " + typeof(T).FullName + ".";
                throw new IcebergRestException(message, (int)response.StatusCode, responseText, headers, exception);
            }
        }

        try
        {
            await using Stream responseStream =
                await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            T? typedBody = await JsonSerializer
                .DeserializeAsync<T>(responseStream, JsonSerializerOptions, cancellationToken)
                .ConfigureAwait(false);
            return new ObjectResponseResult<T>(typedBody, string.Empty);
        }
        catch (JsonException exception)
        {
            var message = "Could not deserialize the response body stream as " + typeof(T).FullName + ".";
            throw new IcebergRestException(message, (int)response.StatusCode, string.Empty, headers, exception);
        }
    }

    private async Task<IcebergRestException<T>> HandleError<T>(
        HttpResponseMessage response,
        Dictionary<string, IEnumerable<string>> headers,
        int status,
        string message,
        CancellationToken cancellationToken)
    {
        ObjectResponseResult<T> objectResponse =
            await ReadObjectResponseAsync<T>(response, headers, cancellationToken)
                .ConfigureAwait(false);
        if (objectResponse.Object == null)
            throw new IcebergRestException(
                "Response was null which was not expected.",
                status,
                objectResponse.Text,
                headers,
                null);
        return new IcebergRestException<T>(
            message,
            status,
            objectResponse.Text,
            headers,
            objectResponse.Object,
            null);
    }

    private string ConvertToString(object value, CultureInfo cultureInfo)
    {
        if (value == null) return "";

        if (value is Enum)
        {
            var name = Enum.GetName(value.GetType(), value);
            if (name != null)
            {
                FieldInfo? field = value.GetType().GetTypeInfo().GetDeclaredField(name);
                if (field != null)
                    if (field.GetCustomAttribute<JsonStringEnumMemberNameAttribute>() is { } attribute)
                        return attribute.Name ?? name;

                var converted =
                    Convert.ToString(Convert.ChangeType(value, Enum.GetUnderlyingType(value.GetType()), cultureInfo));
                return converted ?? string.Empty;
            }
        }
        else if (value is bool b)
        {
            return Convert.ToString(b, cultureInfo).ToLowerInvariant();
        }
        else if (value is byte[] bytes)
        {
            return Convert.ToBase64String(bytes);
        }
        else if (value is string[] strings)
        {
            return string.Join(",", strings);
        }
        else if (value.GetType().IsArray)
        {
            Array valueArray = (Array)value;
            var valueTextArray = new string[valueArray.Length];
            for (var i = 0; i < valueArray.Length; i++)
                valueTextArray[i] = ConvertToString(valueArray.GetValue(i), cultureInfo);
            return string.Join(",", valueTextArray);
        }

        var result = Convert.ToString(value, cultureInfo);
        return result ?? "";
    }

    private readonly struct ObjectResponseResult<T>(T responseObject, string responseText)
    {
        public T Object { get; } = responseObject;

        public string Text { get; } = responseText;
    }
}

#pragma warning restore 1573
#pragma warning restore 1591