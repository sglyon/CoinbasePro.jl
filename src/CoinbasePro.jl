module CoinbasePro

using HTTP, JSON, DataFrames, Parameters, Dates, Base64, SHA, Printf, Decimals

const HOST = "https://api.pro.coinbase.com"
_dropmissing(x) = x
_dropmissing(d::Dict) = Dict(k => v for (k, v) in d if !ismissing(v))

struct AuthenticatedClient
    api_key::String
    secret_key::String
    passphrase::String
end

function auth_headers(c::AuthenticatedClient, method::String, endpoint::String, body::String)
    # CB-ACCESS-KEY The api key as a string.
    # CB-ACCESS-SIGN The base64-encoded signature (see Signing a Message).
    # CB-ACCESS-TIMESTAMP A timestamp for your request.
    # CB-ACCESS-PASSPHRASE The passphrase you specified when creating the API key.
    timestamp = @sprintf "%f" datetime2unix(now(UTC))
    message = string(timestamp, method, endpoint, body)
    hmac_key = base64decode(c.secret_key)
    signature = hmac_sha256(hmac_key, message)
    return Dict(
        "CB-ACCESS-KEY" => c.api_key,
        "CB-ACCESS-SIGN" => base64encode(signature),
        "CB-ACCESS-TIMESTAMP" => timestamp,
        "CB-ACCESS-PASSPHRASE" => c.passphrase,
        "Content-Type" => "application/json",
    )
end

function _handle_response(res::HTTP.Response)
    if res.status >= 300
        @error "Bad request $r"
    end
    return JSON.parse(String(res.body))
end

function _request(c::AuthenticatedClient, method::String, endpoint::String, params::Dict=Dict(), data::Dict=Dict())
    url = string(HOST, endpoint)
    body = length(data) > 0 ? JSON.json(data) : ""
    headers = auth_headers(c, method, endpoint, body)
    _handle_response(
        HTTP.request(method, url, headers, body, query=params)
    )
end

function _request(method::String, endpoint::String, params::Dict=Dict(), data::Dict=Dict())
    url = string(HOST, endpoint)
    _handle_response(
        HTTP.request(method, url, ["Content-Type" => "application/json"], JSON.json(data), query=params)
    )
end

get_products() = _request("GET", "/products")
get_product_order_book(product_id::String, level::Int=1) =
    _request("GET", string("/products/", product_id, "/book"), Dict(:level => level))
get_product_ticker(product_id::String) = _request("GET", "/products/$product_id/ticker")

# function get_product_trades(product_id::String, before::String="",
#     after::String="",
#     limit::Union{missing,Int}=missing,
#     result::Union{Missing,Int}=missing,)
#     _request(c, "GET", "/products/$product_id/ticker")
# end

function get_product_historic_rates(
        product_id::String;
        start::Union{Missing,String}=missing,
        stop::Union{Missing,String}=missing,
        granularity::Union{Missing,Int}=missing,
    )
    params = Dict()
    !ismissing(start) && setindex!(params, start, :start)
    !ismissing(stop) && setindex!(params, stop, :end)
    if !ismissing(granularity)
        accepted_grans = [60, 300, 900, 3600, 21600, 86400]
        if !(granularity in accepted_grans)
            error("Specified granularity is $granularity, must be in approved values: $accepted_grans")
        end
        params[:granularity] = granularity
    end
    @show params
    res = _request("GET", "/products/$product_id/candles", params)
    rows = []
    for row in res
        push!(rows, (dt = Dates.unix2datetime(row[1]), low = row[2], high = row[3], open = row[4], close = row[5], volume = row[6]))
    end
    DataFrame(rows)
end

get_product_24hr_stats(product_id::String) = _request("GET", "/products/$product_id/stats")
get_currencies() = _request("GET", "/currencies")
get_time() = _request("GET", "/time")

## auth methods
get_orders(c::AuthenticatedClient; product_id::Union{Missing,String}=missing, order_id::Union{Missing,String}=missing) =
    _request(c, "GET", "/orders", _dropmissing(Dict("product_id" => product_id, "order_id" => order_id)))

get_accounts(c::AuthenticatedClient) = _request(c, "GET", "/accounts/")

convert_stablecoin(c::AuthenticatedClient, from::String, to::String, amount::Union{Integer,Decimals.Decimal}) =
    _request(c, "POST", "/conversions", Dict(), Dict("from" => from, "to" => to, "amount" => amount))



end # module
