using Bogus;
using System.Data;
using System.Data.SqlClient;
using System.Net;

namespace ConsoleApp1
{
    class Program
    {
        static List<string> TABLE_TO_SKIP = new List<string> { "dbo.__EFMigrationsHistory", "dbo.GeneralSettings", "dbo.Bancos", "dbo.Convenios", "dbo.Robos", "dbo.RobosFluxo", "dbo.TipoConta" };
        static List<string> COLUMNS_TO_SKIP = new List<string> { "id", "creationdate", "creationuser", "changedate", "changeuser", "isactive", "masterid" };
        static List<string> SCHEMAS = new List<string> { "'basic', 'dbo'" };
        static int NUMER_OF_ITEMS = 10;
        static bool APPEND_DATA = false;

        static Dictionary<string, Type> _typeAlias = new Dictionary<string, Type> {
            {  "bit" , typeof(bool)},
            {  "char" ,typeof(char)},
            {  "decimal" ,typeof(decimal)},
            {  "double" ,typeof(double)},
            {  "float" ,typeof(float)},
            {  "int" ,typeof(int)},
            {  "long" ,typeof(long)},
            {  "short" ,typeof(short)},
            {  "string" ,typeof(string)},
            {  "varchar",typeof(string) },
            {  "nvarchar",typeof(string) },
            {  "varbinary",typeof(byte[]) },
            {  "date",typeof(DateTime) },
            {  "datetimeoffset",typeof(DateTime) },
            {  "datetime",typeof(DateTime) },
            {  "datetime2",typeof(DateTime) },
        };

        static async Task Main(string[] args)
        {
            // Conexão com o banco de dados
            //
            //var connectionString = "";
            
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            // Consulta as tabelas do banco de dados
            var tables = new List<string>();
            using var command = new SqlCommand($"SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ({string.Join(",", SCHEMAS)}) ORDER BY TABLE_NAME", connection);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                tables.Add(reader.GetString(0));
            }
            reader.Close();

            // Loop pelas tabelas e colunas e gera os dados fictícios
            foreach (var tableName in tables)
            {
                var count = await GetTableCount(connection, tableName);

                if (count > 0 && !APPEND_DATA)
                {
                    Console.WriteLine($"{tableName} already has data. Ignoring.");
                    continue;
                }
                    
                
                if (!TABLE_TO_SKIP.Any(x => x.ToUpper() == tableName.ToUpper()))
                    await HandleTable(connection, tableName);
            }
        }

        private static async Task HandleTable(SqlConnection connection, string tableName)
        {
            Console.WriteLine($"Start {tableName}");
            // Configuração do Faker
            var faker = new Faker("pt_BR");

            var columns = new List<DataColumn>();
            var PKColumns = new Dictionary<string, string>();
            var columnsQuery = $"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA + '.' + TABLE_NAME = '{tableName}'";
            using var schemaCommand = new SqlCommand(columnsQuery, connection);
            using var schemaReader = await schemaCommand.ExecuteReaderAsync();
            while (await schemaReader.ReadAsync())
            {
                var columnName = schemaReader.GetString(schemaReader.GetOrdinal("COLUMN_NAME"));
                var isNullable = schemaReader.GetString(schemaReader.GetOrdinal("IS_NULLABLE")) == "YES";
                //var dataType = Type.GetType(schemaReader.GetString(schemaReader.GetOrdinal("DATA_TYPE")));
                var sqlType = schemaReader.GetString(schemaReader.GetOrdinal("DATA_TYPE"));
                _typeAlias.TryGetValue(sqlType, out Type dataType);

                if (COLUMNS_TO_SKIP.Any(x => x.ToUpper() == columnName.ToUpper()))
                    continue;

                if (dataType == null)
                    throw new Exception($"there is no type for {sqlType}");

                if (dataType == typeof(string))
                {
                    var maxLength = schemaReader.GetInt32(schemaReader.GetOrdinal("CHARACTER_MAXIMUM_LENGTH"));
                    columns.Add(new DataColumn(columnName, typeof(string)) { MaxLength = maxLength });
                }
                else
                {
                    columns.Add(new DataColumn(columnName, dataType) { AllowDBNull = false });
                }
            }

            schemaReader.Close();

            var dictPKResult = await HandleChildrens(connection, tableName);
            foreach (var item in dictPKResult)
                PKColumns.Add(item.Key, item.Value);

            // Gera os dados fictícios
            var dataTable = new DataTable(tableName);
            foreach (var column in columns)
            {
                dataTable.Columns.Add(column);
            }
            for (int i = 0; i < NUMER_OF_ITEMS; i++)
            {
                //Console.WriteLine($"Add item {i} to {tableName}");
                var row = dataTable.NewRow();
                foreach (var column in columns)
                {
                    if (COLUMNS_TO_SKIP.Any(x => x == column.ColumnName))
                        continue;

                    //if (column.AllowDBNull && faker.Random.Bool())
                    //{
                    //    row[column] = DBNull.Value;
                    //}
                    if (column.DataType == typeof(string))
                    {
                        var stringValue = string.Empty;

                        if(column.ColumnName.ToUpper().Contains("NOME"))
                            stringValue = faker.Name.FullName().Replace("'", "*").Replace(",", "*");
                        else if (column.ColumnName.ToUpper().Contains("EXTENSION"))
                            stringValue = faker.System.CommonFileExt();
                        else if (column.ColumnName.ToUpper().Contains("FILENAME"))
                            stringValue = Path.GetFileNameWithoutExtension(faker.System.CommonFileName());
                        else if (column.ColumnName.ToUpper().Contains("ISS") ||
                                 column.ColumnName.ToUpper().Contains("CNPJ") ||
                                 column.ColumnName.ToUpper().Contains("CPF"))
                            stringValue = faker.Random.Int(0,int.MaxValue).ToString().Replace("'", "*").Replace(",", "*");
                        else
                            stringValue = faker.WaffleText(faker.Random.Int(1, 10)).Replace("'", "*").Replace(",", "*");

                        var columnLength = column.MaxLength <= 0 ? 1 : column.MaxLength;
                        if (stringValue.Length > columnLength)
                            stringValue = stringValue.Substring(0, columnLength);
                        row[column] = stringValue;
                    }
                    else if (column.DataType == typeof(int))
                    {
                        if (PKColumns.TryGetValue(column.ColumnName, out var pkTableName))
                        {
                            var count = await GetTableCount(connection, tableName);
                            if (count == 0)
                            {
                                await HandleTable(connection, pkTableName);
                            }

                            var pkquery = $"SELECT TOP 1 Id FROM {pkTableName} ORDER BY NEWID()";
                            using var pkCommand = new SqlCommand(pkquery, connection);
                            var pkId = (int)await pkCommand.ExecuteScalarAsync();
                            row[column] = pkId;
                        }
                        else
                        {
                            row[column] = faker.Random.Int(1, 10);
                        }
                    }
                    else if (column.DataType == typeof(decimal))
                    {
                        row[column] = faker.Finance.Amount(0, 10, 4);
                    }
                    else if (column.DataType == typeof(DateTime))
                    {
                        row[column] = faker.Date.Recent();
                    }
                    else if (column.DataType == typeof(bool))
                    {
                        row[column] = faker.Random.Bool();
                    }
                    else if (column.DataType == typeof(byte[]))
                    {
                        var imageUrl = faker.Image.PlaceImgUrl();
                        using (var webClient = new WebClient())
                        {
                            var imageBytes = webClient.DownloadData(imageUrl);
                            row[column] = imageBytes;
                        }
                    }
                }
                dataTable.Rows.Add(row);
            }

            // Monta o script de inserção
            var insertQuery = $"INSERT INTO {tableName} ({string.Join(", ", columns.Select(c => $"[{c.ColumnName}]"))}) VALUES ";
            for (int i = 0; i < dataTable.Rows.Count; i++)
            {
                var rowValues = new List<string>();
                foreach (var column in columns)
                {
                    var value = dataTable.Rows[i][column];
                    if (value == DBNull.Value)
                    {
                        rowValues.Add("NULL");
                    }
                    else if (column.DataType == typeof(string) || column.DataType == typeof(DateTime) || column.DataType == typeof(bool))
                    {
                        rowValues.Add($"'{value}'");
                    }
                    else if (column.DataType == typeof(byte[]))
                    {
                        byte[] byteArray = value as byte[];
                        rowValues.Add($"CAST('{Convert.ToBase64String(byteArray)}' AS VARBINARY(MAX))");
                    }
                    else
                    {
                        rowValues.Add(value.ToString());
                    }
                }
                insertQuery += $"({string.Join(", ", rowValues)}),";
            }
            insertQuery = insertQuery.TrimEnd(',') + ";";

            using var insertCommand = new SqlCommand(insertQuery, connection);
            await insertCommand.ExecuteScalarAsync();

            //Console.WriteLine($"Finish {tableName}");
        }

        private static async Task<Dictionary<string, string>> HandleChildrens(SqlConnection connection, string tableName)
        {
            var PKColumns = new Dictionary<string, string>();

            var selectQuery = $"SELECT KU.COLUMN_NAME, KU.TABLE_SCHEMA + '.' + KU.TABLE_NAME, KU.CONSTRAINT_NAME, " +
                              $"KU2.COLUMN_NAME AS REFERENCED_COLUMN_NAME, " +
                              $"KU2.TABLE_SCHEMA + '.' + KU2.TABLE_NAME AS REFERENCED_TABLE_NAME " +
                              $"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KU " +
                              $"INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC ON KU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME " +
                              $"INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KU2 ON RC.UNIQUE_CONSTRAINT_NAME = KU2.CONSTRAINT_NAME " +
                              $"WHERE KU.TABLE_SCHEMA + '.' + KU.TABLE_NAME = '{tableName}' AND KU.CONSTRAINT_NAME LIKE 'FK_%'";
            using var selectCommand = new SqlCommand(selectQuery, connection);
            using var foreignKeyReader = await selectCommand.ExecuteReaderAsync();

            while (await foreignKeyReader.ReadAsync())
            {
                var columnName = foreignKeyReader.GetString(foreignKeyReader.GetOrdinal("COLUMN_NAME"));
                var referencedTableName = foreignKeyReader.GetString(foreignKeyReader.GetOrdinal("REFERENCED_TABLE_NAME"));
                var referencedColumnName = foreignKeyReader.GetString(foreignKeyReader.GetOrdinal("REFERENCED_COLUMN_NAME"));

                if (tableName == referencedTableName)
                    continue;

                // Verifica se há dados na tabela referenciada
                var count = await GetTableCount(connection, tableName);
                if (count == 0 && !TABLE_TO_SKIP.Any(x => x == tableName))
                {
                    // Adicionar Dados
                    await HandleTable(connection, referencedTableName);
                }
                PKColumns.Add(columnName, referencedTableName);
            }

            return PKColumns;
        }

        private static async Task<int> GetTableCount(SqlConnection connection, string tableName)
        {
            try
            {
                var countQuery = $"SELECT COUNT(1) FROM {tableName}";
                using var countCommand = new SqlCommand(countQuery, connection);
                var result = await countCommand.ExecuteScalarAsync();

                if (result != null && int.TryParse(result.ToString(), out int countResult))
                    return countResult;

                throw new Exception($"Error getting count from {tableName}");
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
