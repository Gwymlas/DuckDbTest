using DuckDB.NET.Data;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Npgsql;

namespace DuckDbTest;

internal class Program
{
    private static void TransferDataToDuckDbUsingSelect(DuckDBConnection duckDbConnection)
    {
        using var command = duckDbConnection.CreateCommand();
        
        command.CommandText = "DROP TABLE IF EXISTS duckdb_geom";
        command.ExecuteNonQuery();
        
        command.CommandText = @"
            CREATE TABLE duckdb_geom AS
            SELECT id, ST_GeomFromWKB(geom) as geom FROM postgres_db.geom_test";
        command.ExecuteNonQuery();
    }
    
    private static void TestNtsForDuckDbDataAsWkb(DuckDBConnection duckDbConnection)
    {
        using var command = duckDbConnection.CreateCommand();

        command.CommandText = "SELECT id, ST_AsWKB(geom) as geom_wkb FROM duckdb_geom";

        var wkbReader = new WKBReader();
        var geometries = new List<Geometry>();

        Console.WriteLine("\nРабота с геометрией через NTS:");
        using var duckDbReader = command.ExecuteReader();
        while (duckDbReader.Read())
        {
            var wkbBytes = duckDbReader.GetStream(1);
            var geometry = wkbReader.Read(wkbBytes);
            geometries.Add(geometry);
            
            Console.WriteLine($"Тип геометрии: {geometry.GeometryType}, Площадь: {geometry.Area}");
        }
        
        var distance = geometries[0].Distance(geometries[1]);
        Console.WriteLine($"Расстояние между первым и вторым элементами: {distance}");
        
        var crosses = geometries[0].Crosses(geometries[1]);
        Console.WriteLine($"Пересекает ли: {(crosses? "Да" : "Нет")}");
    }
    
    private static void Main()
    {
        const string host = "localhost";
        const int port = 5444;
        const string dbname = "test";
        const string user = "postgres";
        const string password = "123";

        const string duckDbFile = "duck_wkb.db";

        var postgresConnectionStringForDuckDb =
            $"host={host} port={port} dbname={dbname} user={user} password={password}";
        var postgresConnectionString = $"Host={host};Port={port};Database={dbname};Username={user};Password={password}";
        
        CreatePostgresTable(postgresConnectionString);

        using var duckDbConnection = new DuckDBConnection($"Data Source={duckDbFile}");
        duckDbConnection.Open();

        using var command = duckDbConnection.CreateCommand();
        
        // Подключение PostgreSQL к DuckDb в режиме только для чтения
        command.CommandText = $"INSTALL postgres; LOAD postgres; ATTACH '{postgresConnectionStringForDuckDb}' AS postgres_db (TYPE postgres, READ_ONLY)";
        command.ExecuteNonQuery();
        
        //Включение пространственного расширения
        command.CommandText = "INSTALL spatial; LOAD spatial";
        command.ExecuteNonQuery();
        
        // TransferDataToDuckDbUsingSelect(duckDbConnection);
        TransferDataToDuckDbUsingCopy(duckDbConnection);
        
        // PrintDataToConsole(duckDbConnection);
        
        TestNtsForDuckDbData(duckDbConnection);
        
        duckDbConnection.Close();
    }

    private static void CreatePostgresTable(string connectionString)
    {
        using var postgresConnection = new NpgsqlConnection(connectionString);
        postgresConnection.Open();

        using var command = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS postgis;", postgresConnection);
        command.ExecuteNonQuery();
        
        command.CommandText = @"
            CREATE TABLE IF NOT EXISTS geom_test (
                id SERIAL PRIMARY KEY,
                geom GEOMETRY
            )";
        command.ExecuteNonQuery();

        command.CommandText = @"
            INSERT INTO geom_test (geom) VALUES 
            (ST_GeomFromText('POLYGON((19.65622766921524 54.4686443760213, 
                                  19.65622766921524 54.46830195180331, 
                                  19.656796427736964 54.46830195180331, 
                                  19.656796427736964 54.4686443760213, 
                                  19.65622766921524 54.4686443760213))', 4326)),
            (ST_GeomFromText('POLYGON((50.17541970055876 53.21416021499951, 
                                  50.17533053703244 53.2141271619239, 
                                  50.17522014409445 53.214205980754855, 
                                  50.17493142410382 53.21407631098768, 
                                  50.17529232409254 53.21377120410435, 
                                  50.17558104408323 53.21389578967748, 
                                  50.17550886408509 53.21396952382588, 
                                  50.1756065193764 53.21402291744005, 
                                  50.17541970055876 53.21416021499951))', 4326))";
        command.ExecuteNonQuery();
        
        Console.WriteLine("Тестовая таблица создана в PostgreSQL");
        postgresConnection.Close();
    }
    
    private static void TransferDataToDuckDbUsingCopy(DuckDBConnection duckDbConnection)
    {
        using var command = duckDbConnection.CreateCommand();
        
        command.CommandText = "COPY (SELECT id, geom FROM postgres_db.geom_test) TO 'data.parquet' (FORMAT parquet);";
        command.ExecuteNonQuery();
        
        command.CommandText = @"CREATE OR REPLACE TABLE duckdb_geom AS 
            SELECT id, ST_AsWKB(geom) as geom FROM 'data.parquet';";
        command.ExecuteNonQuery();
    }

    private static void PrintDataToConsole(DuckDBConnection duckDbConnection)
    {
        using var command = duckDbConnection.CreateCommand();
        
        command.CommandText = "SELECT id, ST_AsText(geom) as geom FROM duckdb_geom";
        using var duckDbReader = command.ExecuteReader();
        Console.WriteLine("\nДанные в DuckDB:");
        while (duckDbReader.Read())
            Console.WriteLine($"ID: {duckDbReader.GetInt32(0)}, Geometry: {duckDbReader.GetString(1)}");
    }
    
    private static void TestNtsForDuckDbData(DuckDBConnection duckDbConnection)
    {
        using var command = duckDbConnection.CreateCommand();

        command.CommandText = "SELECT geom FROM duckdb_geom";
        // command.CommandText = "SELECT geom FROM postgres_db.geom_test";

        using var duckDbReader = command.ExecuteReader();
        var wkbReader = new WKBReader();
        
        var postGisReader = new PostGisReader();
        // var geoPackageGeoReader = new GeoPackageGeoReader();
        // var tinyWkbReader = new TinyWkbReader();
        // var gaiaGeoReader = new GaiaGeoReader();

        while (duckDbReader.Read())
        {
            var duckDbStream = duckDbReader.GetStream(0);
            // byte[] DuckDbGeometryBytes = new byte[0];
            byte[] DuckDbGeometryBytes = new byte[duckDbStream.Length];
            duckDbStream.Read(DuckDbGeometryBytes, 0, DuckDbGeometryBytes.Length);

            // duckDbStream.Seek(0, SeekOrigin.Begin);
            // var geometry = wkbReader.Read(duckDbStream);
            
            duckDbStream.Close();
            
    
            Geometry? geom = null;
            try 
            {
                geom = postGisReader.Read(DuckDbGeometryBytes);
                Console.WriteLine("Успешное чтение через PostGIS-формат");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка PostGIS формата: {ex.Message}");
            }
            
            Console.WriteLine($"SRID: {geom?.SRID}");
        }
    }
}