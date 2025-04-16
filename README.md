# Mofucat.DataToolkit

[![NuGet](https://img.shields.io/nuget/v/Mofucat.DataToolkit.svg)](https://www.nuget.org/packages/Mofucat.DataToolkit)

## ObjectDataReader

```csharp
// Object
var list = new List<Data>
[
    new() { Id = 1, Value = "Name-1" },
    new() { Id = 2 }
];

// ReaderAdapter 
using var reader = new ObjectDataReader<Data>(list);

// BulkCopy
await using var con = new SqlConnection(ConnectionString);
await con.OpenAsync();
using var loader = new SqlBulkCopy(con);
loader.DestinationTableName = "Data";
await loader.WriteToServerAsync(reader);
```

## MappingDataReader

(TODO)

# Mofucat.DataToolkit.Avro

[![NuGet](https://img.shields.io/nuget/v/Mofucat.DataToolkit.Avro.svg)](https://www.nuget.org/packages/Mofucat.DataToolkit.Avro)

## AvroDataReader

```csharp
// ReaderAdapter 
using var reader = new AvroDataReaderAdapter(File.OpenRead("data.avro"));

// BulkCopy
await using var con = new SqlConnection(ConnectionString);
await con.OpenAsync();
using var loader = new SqlBulkCopy(con);
loader.DestinationTableName = "Data";
await loader.WriteToServerAsync(reader);
```

## AvroDataExporter

(TODO)
