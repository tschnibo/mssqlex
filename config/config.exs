import Config



# Config custom drivers in your projects config! Pattern:
#

for config <- "config/config.exs" |> Path.expand(__DIR__) |> Path.wildcard() do
  import_config config
end


config :mssqlex,
  drivername_pingstatement: [%{"name" => ExampleConn, "ping_statement" => "SELECT 1"}]