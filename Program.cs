using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Xml.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Extensions.Logging;
using System.Linq;


namespace Marjani.Importer
{
    class Program
    {

        private static readonly string configFileName = "config.json";
        private static readonly string stateFileName = "state.json";
        private static readonly string dateFormat = "yyyy-MM-dd";

        private static readonly string newsStateFileName = "newsState.json";
        private static readonly string dlStateFileName = "dlState.json";
        private static readonly string mapFileName = "map.json";
        private static readonly char spliter = '$';
        private static Dictionary<long, long> map = new Dictionary<long, long>();
        private static NewsState newsState = null;
        private static DlState dlState = null;
        private static Config config = null;
        private static ILogger<Runner> logger = null;
        private static PgSqlHelper pgSqlHelprt;
        static void Main(string[] args)
        {
            #region  Log Config DI
            var servicesProvider = BuildDi();
            var runner = servicesProvider.GetRequiredService<Runner>();
            logger = runner._logger;
            #endregion


            logger.LogInformation("Importer App Started");

            #region Read config
            logger.LogInformation("Looking for config file");
            if (!System.IO.File.Exists(configFileName))
            {
                logger.LogError("Config file not found on " + Directory.GetCurrentDirectory());
                Console.WriteLine("Press ANY key to exit");
                return;
            }
            logger.LogInformation("Config file found, reading configuration ...");
            var strConfigs = System.IO.File.ReadAllText(configFileName);
            config = Newtonsoft.Json.JsonConvert.DeserializeObject<Config>(strConfigs);
            logger.LogInformation("Config is:" + Environment.NewLine);
            logger.LogInformation(strConfigs);
            #endregion

            #region Read state
            logger.LogInformation("Looking for state file");
            if (!System.IO.File.Exists(stateFileName))
            {
                logger.LogError("State file not found on " + Directory.GetCurrentDirectory());
                Console.WriteLine("Press ANY key to exit");
                return;
            }
            logger.LogInformation("Config file found, reading configuration ...");
            var strState = System.IO.File.ReadAllText(stateFileName);
            var state = Newtonsoft.Json.JsonConvert.DeserializeObject<State>(strState);
            logger.LogInformation("State is:" + Environment.NewLine);
            logger.LogInformation(strState);


            #region newsState
            logger.LogInformation("Looking for  newsState file");
            if (!System.IO.File.Exists(newsStateFileName))
            {
                logger.LogError("newsState file not found on " + Directory.GetCurrentDirectory());
                Console.WriteLine("Press ANY key to exit");
                return;
            }
            logger.LogInformation("newsState file found, reading configuration ...");
            var strNewsState = System.IO.File.ReadAllText(newsStateFileName);
            newsState = Newtonsoft.Json.JsonConvert.DeserializeObject<NewsState>(strNewsState);
            logger.LogInformation("newsState is:" + Environment.NewLine);
            logger.LogInformation(strNewsState);
            #endregion

            #region dlState
            logger.LogInformation("Looking for  dlState file");
            if (!System.IO.File.Exists(dlStateFileName))
            {
                logger.LogError("dlState file not found on " + Directory.GetCurrentDirectory());
                Console.WriteLine("Press ANY key to exit");
                return;
            }
            logger.LogInformation("newsState file found, reading configuration ...");
            var strDlState = System.IO.File.ReadAllText(dlStateFileName);
            dlState = Newtonsoft.Json.JsonConvert.DeserializeObject<DlState>(strDlState);
            logger.LogInformation("dlState is:" + Environment.NewLine);
            logger.LogInformation(strDlState);
            #endregion

            #region map
            logger.LogInformation("Looking for  dlState file");
            if (!System.IO.File.Exists(mapFileName))
            {
                logger.LogError("dlState file not found on " + Directory.GetCurrentDirectory());
                Console.WriteLine("Press ANY key to exit");
                return;
            }
            logger.LogInformation("newsState file found, reading configuration ...");
            var strMapState = System.IO.File.ReadAllText(mapFileName);
            map = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<long,long>>(strMapState);
            logger.LogInformation("map is:" + Environment.NewLine);
            logger.LogInformation(strMapState);
            #endregion

            #endregion

            Thread updateNewsThread = new Thread(new ThreadStart(UpdateNews));
            Thread updateDlThread = new Thread(new ThreadStart(UpdateDl));

            updateNewsThread.Start();
            updateDlThread.Start();

            Console.WriteLine("End of prog");
            Console.ReadKey();
            // Ensure to flush and stop internal timers/threads before application-exit (Avoid segmentation fault on Linux)
            NLog.LogManager.Shutdown();

        }

        private static void UpdateNews()
        {
            pgSqlHelprt = new PgSqlHelper(config, logger);

            #region Find folders to visit
            var firstFolder = newsState.NewsId / 1000;
            var directoriesNumber = new List<int>();
            var directories = System.IO.Directory.GetDirectories(config.RepositoryPath);

            if (directories != null && directories.Count() > 0)
            {
                foreach (var dic in directories)
                {
                    int dicNumber = 0;
                    if (int.TryParse(Path.GetDirectoryName(dic), out dicNumber))
                    {
                        if (dicNumber >= firstFolder)
                        {
                            directoriesNumber.Add(dicNumber);
                        }
                    }
                }
            }
            #endregion

            #region Find news to visit
            var newsToVisit = new List<int>();
            if (directoriesNumber != null && directoriesNumber.Count > 0)
            {
                #region news in First folder
                newsToVisit.AddRange(
                     System.IO.Directory.GetFiles(Path.Combine(config.RepositoryPath, directoriesNumber[0].ToString()), "*.xml")
                     .Select(o => int.Parse(Path.GetFileNameWithoutExtension(o)))
                     .Where(o => o > newsState.NewsId).ToList()
                );
                #endregion
                #region  news in other folder
                for (int i = 1; i < directoriesNumber.Count; i++)
                {
                    newsToVisit.AddRange(
                    System.IO.Directory.GetFiles(Path.Combine(config.RepositoryPath, directoriesNumber[i].ToString()), "*.xml")
                     .Select(o => int.Parse(Path.GetFileNameWithoutExtension(o))).ToList()
                    );
                }
                #endregion
            }
            #endregion

            #region Loop on news
            if (newsToVisit != null && newsToVisit.Count > 0)
            {
                foreach (var item in newsToVisit)
                {
                    var currentFolder = item / 1000;
                    logger.LogInformation("Start to work on ['folder':'" + (item / 1000).ToString() + "', 'File':'" + item + "']");
                    var xmlFilePath = Path.Combine(config.RepositoryPath, (item / 1000).ToString(), item + ".xml");
                    if (!System.IO.File.Exists(xmlFilePath))
                    {
                        logger.LogWarning("File " + item + ".xml not fount in " + currentFolder + ", continu to next line");
                        UpdateNewsState(item);
                        continue;
                    }
                    RasadNews rasadNews = null;
                    #region  Pars XML
                    try
                    {
                        var xmlText = System.IO.File.ReadAllText(xmlFilePath);
                        xmlText = System.Net.WebUtility.HtmlDecode(xmlText);
                        XmlSerializer serializer = new XmlSerializer(typeof(RasadNews));

                        using (TextReader reader = new StringReader(xmlText))
                        {
                            rasadNews = (RasadNews)serializer.Deserialize(reader);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError("Pars xml file(" + item + ") error, ex:" + ex.Message);
                        UpdateNewsState(item);
                        continue;
                    }
                    if (rasadNews == null)
                    {
                        logger.LogError("Pars xml file(" + item + ") error, result is null");
                        UpdateNewsState(item);
                        continue;
                    }
                    #endregion
                    try
                    {
                        var insertedNewsId = pgSqlHelprt.AddRasadNews(rasadNews, currentFolder.ToString(), item);
                        if (insertedNewsId.HasValue)
                        {
                            AddMapState(item, insertedNewsId.Value);
                        }
                    }
                    catch (Exception ex)
                    {

                    }
                    UpdateNewsState(item);
                }
            }
            #endregion
        }

        private static void UpdateDl()
        {
            pgSqlHelprt = new PgSqlHelper(config, logger);

            var dlFileList = Path.Combine(config.RepositoryPath, "dl.txt");
            var lines = File.ReadLines(dlFileList).Skip(dlState.Line).ToList();
            foreach (var item in lines)
            {
                var newsId = int.Parse(item.Split(spliter)[0]);
                var bytez = File.ReadAllBytes(Path.Combine(config.RepositoryPath, (newsId / 1000).ToString(), "dl", item));
                pgSqlHelprt.InsertAttachment(newsId, item.Split(spliter).LastOrDefault(), bytez);
                UpdateDlState(dlState.Line++);
            }

        }

        private static void EndOfProg()
        {
            Console.WriteLine("Press ANY key to exit");
            return;
        }

        private static void Check()
        { }
        private static void UpdateState(string folder, int line)
        {
            var strState = Newtonsoft.Json.JsonConvert.SerializeObject(new State()
            {
                LastFolder = folder,
                LastLine = line
            });
            System.IO.File.WriteAllText(stateFileName, strState);
        }

        private static void UpdateNewsState(int newsId)
        {
            newsState = new NewsState()
            {
                NewsId = newsId
            };
            var strState = Newtonsoft.Json.JsonConvert.SerializeObject(newsState);
            System.IO.File.WriteAllText(newsStateFileName, strState);
        }

        private static void UpdateDlState(int line)
        {
            dlState = new DlState()
            {
                Line = line
            };
            var strState = Newtonsoft.Json.JsonConvert.SerializeObject(dlState);
            System.IO.File.WriteAllText(dlStateFileName, strState);
        }

        private static void AddMapState(long newsId, long news_id)
        {
            if (!map.ContainsKey(newsId))
            {
                map.Add(newsId, news_id);
                var strState = Newtonsoft.Json.JsonConvert.SerializeObject(map);
                System.IO.File.WriteAllText(mapFileName, strState);
            }
        }
        private static ServiceProvider BuildDi()
        {
            return new ServiceCollection()
                .AddLogging(builder =>
                {
                    builder.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace);
                    builder.AddNLog(new NLogProviderOptions
                    {
                        CaptureMessageTemplates = true,
                        CaptureMessageProperties = true
                    });
                })
                .AddTransient<Runner>()
                .BuildServiceProvider();
        }
    }
}
