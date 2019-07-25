using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Xml.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Extensions.Logging;

namespace Marjani.Importer
{
    class Program
    {

        private static readonly string configFileName = "config.json";
        private static readonly string stateFileName = "state.json";
        private static readonly string dateFormat = "yyyy-MM-dd";
        private static PgSqlHelper pgSqlHelprt;
        static void Main(string[] args)
        {
            #region  Log Config DI
            var servicesProvider = BuildDi();
            var runner = servicesProvider.GetRequiredService<Runner>();
            var logger = runner._logger;
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
            var config = Newtonsoft.Json.JsonConvert.DeserializeObject<Config>(strConfigs);
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

            #endregion

            #region Find start visit folder
            var startVisitDate = new DateTime();
            if (!string.IsNullOrEmpty(state.LastFolder))
            {
                try
                {

                    startVisitDate = DateTime.ParseExact(state.LastFolder, "yyyy-MM-dd", CultureInfo.InvariantCulture);
                }
                catch
                {
                    logger.LogError("State Config not in correct formate: [yyyy-MM-dd]");
                    return;
                }
            }
            else
            {
                if (config.VisitLastDay > 0)
                {
                    startVisitDate = startVisitDate.AddDays(config.VisitLastDay * -1);
                }
                else
                {
                    logger.LogError("Can't find start visit folder");
                    EndOfProg();
                }
            }
            logger.LogInformation("Start visit folder is " + startVisitDate.ToString("yyyy-MM-dd"));
            #endregion

            #region Find folders to visit

            if (startVisitDate.Date > DateTime.Now.Date)
            {
                logger.LogError("Start visit folder is affter today, check the system clock");
                EndOfProg();
            }
            List<string> foldersToVisit = new List<string>();
            if (startVisitDate.Date == DateTime.Now.Date)
            {
                foldersToVisit.Add(DateTime.Now.ToString(dateFormat));
            }
            else
            {
                var dateConter = startVisitDate;
                while (dateConter.ToString(dateFormat) != DateTime.Now.ToString(dateFormat))
                {
                    foldersToVisit.Add(dateConter.ToString(dateFormat));
                    dateConter = dateConter.AddDays(1);
                    Console.WriteLine(dateConter.ToString(dateFormat));
                }
            }
            if (foldersToVisit.Count > 0)
            {
                logger.LogInformation("Find " + foldersToVisit.Count + " folders for visit");

            }
            else
            {
                logger.LogError("Find nothing folder for visit");
                EndOfProg();
            }
            #endregion

            #region  Find last line
            var lastLine = state.LastLine;
            #endregion

            pgSqlHelprt = new PgSqlHelper(config, logger);
            foreach (var currentFolder in foldersToVisit)
            {
                if (!System.IO.Directory.Exists(Path.Combine(config.RepositoryPath, currentFolder)))
                {
                    logger.LogWarning("Folder "+currentFolder+" not found! continue next folder.");
                    UpdateState(currentFolder,lastLine);
                    continue;
                }
                var listFilePath = Path.Combine(config.RepositoryPath, currentFolder, "list_files.txt");
                if (!System.IO.File.Exists(listFilePath))
                {
                    logger.LogWarning("List files not found in folder " + currentFolder + ", continue with next folder");
                    UpdateState(currentFolder, lastLine);

                    continue;
                }
                logger.LogInformation("List files found in folder " + currentFolder);
                var lines = System.IO.File.ReadAllLines(listFilePath);

                if (lastLine > lines.Length)
                {
                    logger.LogWarning("last line bigger than list file line count, continue with next folder");
                    UpdateState(currentFolder, lastLine);

                    continue;
                }

                for (int i = lastLine; i < lines.Length; i++)
                {
                    var filePX = lines[i];
                    logger.LogInformation("Start to work on ['folder':'" + currentFolder + "', 'line':'" + i + "', 'file':'" + filePX + "']");
                    var xmlFilePath = Path.Combine(config.RepositoryPath, currentFolder, filePX + ".xml");
                    if (!System.IO.File.Exists(xmlFilePath))
                    {
                        logger.LogWarning("File " + filePX + ".xml not fount in " + currentFolder + ", continu to next line");
                        UpdateState(currentFolder, i);
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
                        logger.LogError("Pars xml file(" + filePX + ") error, ex:" + ex.Message);
                        UpdateState(currentFolder, i);

                        continue;
                    }
                    if (rasadNews == null)
                    {
                        logger.LogError("Pars xml file(" + filePX + ") error, result is null");
                        UpdateState(currentFolder, i);
                        continue;
                    }
                    #endregion
                    try
                    {
                        var attachments = new Dictionary<string, byte[]>();
                        var attachmentFolderPath = Path.Combine(config.RepositoryPath, currentFolder, filePX);
                        var files = System.IO.Directory.GetFiles(attachmentFolderPath);
                        if (files != null && files.Length > 0)
                        {
                            foreach (var item in files)
                            {
                                attachments.Add(Path.GetFileName(item), System.IO.File.ReadAllBytes(item));
                            }
                        }
                        pgSqlHelprt.AddRasadNews(rasadNews, attachments, currentFolder, lastLine);
                    }
                    catch (Exception ex)
                    {

                    }


                }
            }
            Console.ReadLine();

            // Ensure to flush and stop internal timers/threads before application-exit (Avoid segmentation fault on Linux)
            NLog.LogManager.Shutdown();

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
