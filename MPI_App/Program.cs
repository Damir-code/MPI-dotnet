using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;
using MPI;
using MySql.Data.MySqlClient;
using System.Diagnostics;
using Newtonsoft.Json;
using MySqlX.XDevAPI.Common;
using Org.BouncyCastle.Security;

public class SampleDbContext : DbContext
{
    public DbSet<TableDbPhoneIp> db_phone_ip { get; set; }
    public DbSet<TableGames> games { get; set; }
    public DbSet<TableLineups> lineups { get; set; }
    public DbSet<TableResults> results { get; set; }


    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        string connectionString = "Server=localhost;Database=mydb;User ID=root;Password=qwerty;";
        optionsBuilder.UseMySql(connectionString, new MySqlServerVersion(new Version(8, 2, 00)));
    }
}

[Table("db_phone_ip")]
public class TableDbPhoneIp
{
    [Key]
    public int? id { get; set; }
    public string? email { get; set; }
    public string? country { get; set; }
    public string? city { get; set; }
    public string? full_name { get; set; }
    public string? phone { get; set; }
    public string? ip { get; set; }
}

[Table("games")]
public class TableGames
{
    internal int? phone;
    internal int? ip;

    [Key]
    public int? game_id { get; set; }
    public string? team { get; set; }
    public string? city { get; set; }
    public int? goals { get; set; }
    public int? own { get; set; }
}
[Table("lineups")]
public class TableLineups
{
    [Key]
    public int? game_id { get; set; }
    public int? player_id { get; set; }
    public char? start { get; set; }
    public char? cards { get; set; }
    public int? time_in { get; set; }
    public int? goals { get; set; }
}
[Table("results")]
public class TableResults
{
    public int? id { get; set; }
    public string? email { get; set; }
    public string? country { get; set; }
    public string? full_name { get; set; }
    public string? phone { get; set; }
    public string? ip { get; set; }
    public int? game_id { get; set; }
    public string? team { get; set; }
    public string? city { get; set; }
    public int? goals { get; set; }
    public int? own { get; set; }
    public int? player_id { get; set; }
    public char? start { get; set; }
    public char? cards { get; set; }
    public int? time_in { get; set; }
}

public class NestedResult
{
    public TableResults t1 { get; set; }
    public TableResults t2 { get; set; }
}

[Table("JoinedResults")]
public class JoinedResults
{
    public TableDbPhoneIp DbPhoneIp { get; set; }
    public TableGames Game { get; set; }
}

[Table("GameLineupsResults")]
public class GameLineupsResults
{
    public TableGames Game { get; set; }
    public TableLineups Lineup { get; set; }
}




class Program
{
    static string? ExecuteQuery(SampleDbContext dbContext, int selectedQuery, int start, int chunkSize)
    {
        IQueryable<object> result;
        switch (selectedQuery)
        {
            case 1:
                result = dbContext.db_phone_ip
                        .Where(entity => entity.id == 9999)
                        .Skip(start).Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            case 2:
                result = dbContext.db_phone_ip
                        .Where(entity => entity.country == "Russia")
                        .Skip(start).Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            case 3:
                result = dbContext.db_phone_ip
                        .Where(entity => entity.email.Contains("macey.biz"))
                        .Skip(start).Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            case 4:
                string startIpAddress = "193.199.0.0";
                string endIpAddress = "193.256.1.1";
                result = dbContext.db_phone_ip
                    .Where(entity => String.Compare(entity.ip, startIpAddress) >= 0 &&
                                     String.Compare(entity.ip, endIpAddress) <= 0);
                return JsonConvert.SerializeObject(result, Formatting.Indented);
            case 5:
                result = dbContext.db_phone_ip
                    .SelectMany(t1 => dbContext.db_phone_ip,
                        (t1, t2) => new { t1, t2 })
                    .Skip(start).Take(chunkSize);

                return JsonConvert.SerializeObject(result);
            case 6:
                result = dbContext.games
                        .Join(dbContext.lineups, game => game.game_id, lineup => lineup.game_id, (game, lineup) => new { game, lineup })
                        .Skip(start)
                        .Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            case 7:
                result = dbContext.db_phone_ip
                .Skip(start).Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            default:
                return null;

        }
    }
    static void PrintGatheredResults(string[] gatheredResults, int selectedQuery)
    {
        switch (selectedQuery)
        {
            case 1:
                foreach (string strGatheredResults in gatheredResults)
                {
                    List<TableDbPhoneIp> list = JsonConvert.DeserializeObject<List<TableDbPhoneIp>>(strGatheredResults);
                    foreach (var item in list)
                    {
                        Console.WriteLine($"{item.id}"
                                         +$"\n {item.email}"
                                         +$"\n {item.country}"
                                         +$"\n {item.city}"
                                         +$"\n {item.full_name}"
                                         +$"\n {item.phone}"
                                         +$"\n {item.ip}");
                    }
                }
                break;
            case 2:
                foreach (string strGatheredResults in gatheredResults)
                {
                    List<TableDbPhoneIp> list = JsonConvert.DeserializeObject<List<TableDbPhoneIp>>(strGatheredResults);
                    foreach (var item in list)
                    {
                        Console.WriteLine($"\n {item.id}"
                                          +$"\n{item.email}"
                                          +$"\n{item.country}"
                                          +$"\n{item.city}"
                                          +$"\n{item.full_name}"
                                          +$"\n{item.phone}"
                                          +$"\n{item.ip} \n");
                    }
                }
                break;
            case 3:
                foreach (string strGatheredResults in gatheredResults)
                {
                    List<TableDbPhoneIp> list = JsonConvert.DeserializeObject<List<TableDbPhoneIp>>(strGatheredResults);
                    foreach (var item in list)
                    {
                        Console.WriteLine($"\n{item.id}"
                                         +$"\n{item.email}"
                                         +$"\n{item.country}"
                                         +$"\n{item.city}"
                                         +$"\n{item.full_name}"
                                         +$"\n{item.phone}"
                                         +$"\n{item.ip}");
                    }
                }
                break;
            case 4:
                foreach (string strGatheredResults in gatheredResults)
                {
                    List<TableDbPhoneIp> list = JsonConvert.DeserializeObject<List<TableDbPhoneIp>>(strGatheredResults);
                    foreach (var item in list)
                    {
                        Console.WriteLine($"\n {item.id}"
                                        +$"\n{item.email}"
                                        +$"\n{item.country}"
                                        +$"\n{item.city}"
                                        +$"\n{item.full_name}"
                                        +$"\n{item.phone}"
                                        +$"\n{item.ip} \n");
                    }
                }
                break;
            case 5:
                    foreach (string strGatheredResults in gatheredResults)
                    { 
                        List<NestedResult> nestedResults = JsonConvert.DeserializeObject<List<NestedResult>>(strGatheredResults);

                        foreach (var nestedResult in nestedResults)
                        {
                                Console.WriteLine($"\n table1"
                                                +$"\n{nestedResult.t1.id}"
                                                +$"\n{nestedResult.t1.email}"
                                                +$"\n{nestedResult.t1.country}"
                                                +$"\n{nestedResult.t1.city}"
                                                +$"\n{nestedResult.t1.full_name}"
                                                +$"\n{nestedResult.t1.phone}"
                                                +$"\n{nestedResult.t1.ip} \n");

                                Console.WriteLine($"\n table2"
                                                +$"\n{nestedResult.t2.id}"
                                                +$"\n{nestedResult.t2.email}"
                                                +$"\n{nestedResult.t2.country}"
                                                +$"\n{nestedResult.t2.city}"
                                                +$"\n{nestedResult.t2.full_name}"
                                                +$"\n{nestedResult.t2.phone}"
                                                +$"\n{nestedResult.t2.ip} \n");
                        }
                    }
        
                break;
            case 6:

                foreach (string strGatheredResults in gatheredResults)
                {
                    List<GameLineupsResults> gameLineupsResults = JsonConvert.DeserializeObject<List<GameLineupsResults>>(strGatheredResults);

                    foreach (var result in gameLineupsResults)
                    {
                        Console.WriteLine($"Game Info:"
                                         + $"{result.Game.game_id}"
                                         + $"{result.Game.team}"
                                         + $"{result.Game.city}"
                                         + $"{result.Game.goals}"
                                         + $"{result.Game.own}");

                        Console.WriteLine($"Lineup Info:"
                                         + $"{result.Lineup.game_id}"
                                         + $"{result.Lineup.player_id}"
                                         + $"{result.Lineup.start}"
                                         + $"{result.Lineup.cards}"
                                         + $"{result.Lineup.time_in}"
                                         + $"{result.Lineup.goals}");
                    }
                }
                break;

            case 7:
                foreach (string strGatheredResults in gatheredResults)
                {
                    List<TableDbPhoneIp> list = JsonConvert.DeserializeObject<List<TableDbPhoneIp>>(strGatheredResults);
                    foreach (var item in list)
                    {
                        Console.WriteLine($"{item.id}"
                                         + $"\n {item.email}"
                                         + $"\n {item.country}"
                                         + $"\n {item.city}"
                                         + $"\n {item.full_name}"
                                         + $"\n {item.phone}"
                                         + $"\n {item.ip}");
                    }
                }
                break;

        }
    }
    static int GetDataSize(MySqlConnection connection, string table)
    {
        using (MySqlCommand command = new MySqlCommand($"SELECT count(*) FROM {table}", connection))
        {
            return (int)(long)command.ExecuteScalar();
        }
    }


    static void Main(string[] args)
    {
        Stopwatch stopwatch = new Stopwatch();

        using (new MPI.Environment(ref args))
        {
            Intracommunicator comm = MPI.Communicator.world;
            int selectedQuery = 0;

            if (comm.Rank == 0)
            {
                while (selectedQuery == 0) { 
                    Console.WriteLine("Выберите SQL-запрос (1-7): ");
                    string? strSelectedQuery = Console.ReadLine();
                    if(string.IsNullOrEmpty(strSelectedQuery))
                    {
                        selectedQuery = 0;
                    }
                    else if(int.TryParse(strSelectedQuery, out int result) && result > 0 && result < 8)
                        {
                            selectedQuery = result;
                        }
                    else { Console.WriteLine("Ошибка ввода!"); }
                }
                stopwatch.Start();
            }

            comm.Broadcast(ref selectedQuery, 0);

            string connectionString = "Server=localhost;Database=mydb;User ID=root;Password=qwerty;";
            MySqlConnection connection = new MySqlConnection(connectionString);
            connection.Open();

            double dataSize = GetDataSize(connection, (selectedQuery == 7) ? "games" : "db_phone_ip");
            int chunkSize = (int)(Math.Ceiling(dataSize / comm.Size));
            int start = comm.Rank * chunkSize;

            using (var dbContext = new SampleDbContext())
            {
                string[] gatheredResults = comm.Gather(ExecuteQuery(dbContext, selectedQuery, start, chunkSize), 0);

                if (comm.Rank == 0)
                {
                    PrintGatheredResults(gatheredResults, selectedQuery);
                    stopwatch.Stop();
                    Console.WriteLine($"Программа выполнялась {stopwatch.ElapsedMilliseconds} миллисекунд.");
                }
            }
            connection.Close();
        }
    }
}

