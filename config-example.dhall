let eq = https://prelude.dhall-lang.org/Natural/equal
let lt = https://prelude.dhall-lang.org/Natural/lessThan
let gt = https://prelude.dhall-lang.org/Natural/greaterThan

let Host = < Host : Text | IPv4 : Text | IPv6 : Text >

let HttpConfig: Type = {
    uri: Text
}

let ConnectionConfig: Type = {
    http: HttpConfig
}

let RegistryConfig: Type = {
    name: Text,
    priority: Natural,
    vendorPrefixes: List Text,
    connection: ConnectionConfig
}

let IgluResolverConfig: Type = {
    cacheSize: Natural,
    cacheTtl: Optional Natural,
    repositories: List RegistryConfig
}

let Config : Type = {
    host: Host,
    port: Natural,
    database: Text,
    username: Text,
    password: Text,
    appName: Text,
    stream: Text,
    igluResolver: IgluResolverConfig
}

let validate: Config -> Bool = \(config: Config) ->
	let maxPortCheck = lt config.port 65535
    in maxPortCheck

let config : Config = {
    host = Host.Host "localhost",
    port = 5432,
    database = "db",
    username = "username",
    password = "password",
    appName = "appName",
    stream = "stream",
    igluResolver = {
        cacheSize = 5,
        cacheTtl = None Natural,
        repositories = [ {
            name = "Iglu Central",
            priority = 0,
            vendorPrefixes = [ "com.snowplowanalytics" ],
            connection = {
                http = {
                    uri = "http://iglucentral.com"
                }
            }
        } ]
    }
}

let valid = assert : validate config === True
in config