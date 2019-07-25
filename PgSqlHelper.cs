using System;
using System.Data;
using Microsoft.Extensions.Logging;
using Npgsql;
using System.Collections.Generic;
public class PgSqlHelper
{
    string connString = "Server={0};Port={1};User Id={2};Password={3};Database={4};";
    NpgsqlConnection conn;
    long? defaultNewsType;
    long? defaultNewsSource;
    bool hasDefualtValue;
    ILogger<Runner> logger;
    public PgSqlHelper(Config config, ILogger<Runner> logger)
    {
        this.logger = logger;
        connString = string.Format(connString, config.DbIp, config.DbPort,
         config.DbUsername, config.DbPassword, config.DbName);
        conn = new NpgsqlConnection(connString);

        hasDefualtValue = GetDefultValues(conn, out defaultNewsType, out defaultNewsSource);
    }

    internal void AddRasadNews(RasadNews rasadNews, Dictionary<string, byte[]> attachments, string folder, int line)
    {
        try
        {
            long rasadNewsSourceId;
            long rasadNewsTypeId;
            if (conn.State == ConnectionState.Closed)
            {
                conn.Open();
            }
            if (rasadNews.SourceId == null && defaultNewsSource == null)
            {
                logger.LogInformation("XML file and defual News SOURCE have not value!");
                return;
            }
            else
            {
                if (rasadNews.SourceId != null)
                {
                    rasadNewsSourceId = GetSourceIdWithCode(rasadNews.SourceId, conn);
                    if (rasadNewsSourceId == 0 && defaultNewsSource == null)
                    {
                        logger.LogInformation("XML file and defual News SOURCE have not value!");
                        return;
                    }
                }
            }

            if (rasadNews.CategoryType == null && defaultNewsType == null)
            {
                logger.LogInformation("XML file and defual News TYPE have not value!");
                return;
            }
            else
            {
                if (rasadNews.CategoryType != null)
                {
                    rasadNewsTypeId = GetTypeIdWithCode(rasadNews.CategoryType, conn);
                    if (rasadNewsTypeId == 0 && defaultNewsType == null)
                    {
                        logger.LogInformation("XML file and defual News TYPE have not value!");
                        return;
                    }
                }
            }

            long forignSourceId = InsertForignSource(rasadNews, folder, line);
            long newsId = InsertNews(rasadNews, forignSourceId);
            if (attachments != null && attachments.Count > 0)
            {
                foreach (var item in attachments)
                {
                    InsertAttachment(newsId, item.Key, item.Value);
                }
            }

        }
        catch (Exception ex)
        {
            logger.LogError("Error on insert news, ex:" + ex.Message);
            return;
        }
        finally
        {
            conn.Close();
        }
    }

    private void InsertAttachment(long newsId, string key, byte[] value)
    {
        var strInsertAttachmentCommand = @"INSERT INTO news.news_attachment(
            news_attachment_id, attachment_description, attachment_file, 
            attachment_file_path, type, userid, bst_suggestion_register_id, 
            news_id, proposed_news_approval_id, attachment_type_id)
    VALUES (nextval('log_seq'), null, '@value', 
            '" + key + @"', null, null, null, 
            '" + newsId + @"', null, null);";
        var cmd = new Npgsql.NpgsqlCommand(strInsertAttachmentCommand, conn);
        cmd.Parameters.AddWithValue("value", value);

        cmd.Parameters.Add(new NpgsqlParameter()
        {

        });
        cmd.ExecuteScalar();
    }

    private long InsertNews(RasadNews rasadNews, long forignSourceId)
    {
        var strInsertNewsCommand = @"INSERT INTO news.news(
            news_id, bst_suggestion_register, consideration, realtime, news_content, 
            declarationdate_time, news_lead, news_level_type, news_number, 
            occurrence_time, register_date, register_news_type, title, approvedorganizationalposition, 
            bst_accuracy_degree_id, bst_confidential_id, bst_refer_level_id, 
            forignsource_forign_source_id, newspic_news_pic_id, organizationalposition_id, 
            news_sources_str, news_types_str, bst_news_type_id)
    VALUES (nextval('log_seq'), null, null, false, '" + rasadNews.Content + @"', 
            '" + rasadNews.PubTime + @"', '" + rasadNews.Description + @"', null, '" + GetStationNumber() + "-" + DateTime.Now.ToString("yyyyMMddHHss") + @"', 
            '" + rasadNews.PubTime + @"', '" + rasadNews.PubTime + @"', 10, '" + rasadNews.Title + @"', null, 
            950, 218, 1, 
            '" + forignSourceId + @"', null, null, 
            null, null, null);";
        var cmd = new Npgsql.NpgsqlCommand(strInsertNewsCommand, conn);
        return (long)cmd.ExecuteScalar();
    }

    private string GetStationNumber()
    {
        throw new NotImplementedException();
    }

    private long InsertForignSource(RasadNews rasadNews, string folder, long line)
    {
        var strInsertForignCommand = @"INSERT INTO news.forign_source(
            forign_source_id, author, category_type, content, description, 
            fetch_time, folder, id, insert_on, line, mesbahdocid, page_url, 
            pub_time, source_id, title, news_id)
    VALUES (nextval('log_seq'), @autor, @CategoryType, @Content, @Description, 
            '" + new DateTime(long.Parse(rasadNews.FetchTime)) + "', '" + folder + "', " + rasadNews.Id + @", '" + DateTime.Now + "', " + line + ", " + rasadNews.mesbahdocid + ", '" + rasadNews.PageUrl + @"', 
            '" + new DateTime(long.Parse(rasadNews.PubTime)) + @"', '" + rasadNews.SourceId + "', @Title, null) RETURNING forign_source_id;";

        var cmd = new Npgsql.NpgsqlCommand(strInsertForignCommand, conn);

        cmd.Parameters.AddWithValue("autor", rasadNews.Author);
        cmd.Parameters.AddWithValue("CategoryType", rasadNews.CategoryType);
        cmd.Parameters.AddWithValue("Content", (rasadNews.Content.Length>10000)?rasadNews.Content.Substring(0,9999):rasadNews.Content);
        cmd.Parameters.AddWithValue("Description", rasadNews.Description);
        cmd.Parameters.AddWithValue("Title", rasadNews.Title);

        var a = cmd.ExecuteScalar();
        return (long)a;
    }

    private long GetTypeIdWithCode(string categoryType, NpgsqlConnection conn)
    {
        try
        {
            NpgsqlCommand cmd;
            cmd = new NpgsqlCommand("select bst_news_type_id from news.bst_news_type where code like '" + categoryType + "' order by bst_news_type_id desc limit 1 ", conn);
            return (long)cmd.ExecuteScalar();
        }
        catch (Exception ex)
        {
            return 0;
        }
    }

    private long GetSourceIdWithCode(string sourceId, NpgsqlConnection conn)
    {
        try
        {
            NpgsqlCommand cmd;
            cmd = new NpgsqlCommand("select news_source_entity_id from news.news_source_entity where code like '" + sourceId + "' order by news_source_entity_id desc limit 1 ", conn); return (long)cmd.ExecuteScalar();
        }
        catch (Exception ex)
        {
            return 0;
        }
    }

    private bool GetDefultValues(NpgsqlConnection conn, out long? defualtNewsType, out long? defualtNewsSource)
    {
        try
        {
            defualtNewsType = null;
            defualtNewsSource = null;
            if (conn.State != ConnectionState.Open)
            {
                conn.Open();
            }
            DataSet ds = new DataSet();
            DataTable dt = new DataTable();
            var strCommand = "SELECT \"generalSystemSettingsId\", \"RASED_BST_NEWS_TYPE_ID\", \"rased_news_Source_Entity_Id\"  FROM news.\"GENERAL_SYSTEM_SETTINGS\" limit 1;";
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(strCommand, conn);
            ds.Reset();
            da.Fill(ds);
            dt = ds.Tables[0];
            if (dt.Rows.Count == 0)
            {
                logger.LogError("General system settings error has not any record!");
                defualtNewsType = null;
                defualtNewsSource = null;
                return false;
            }
            var row = dt.Rows[0];
            if (row["rased_news_source_entity_id"] != null)
            {
                defualtNewsSource = long.Parse(row["rased_news_source_entity_id"].ToString());
            }
            else
            {
                defualtNewsSource = null;
            }

            if (row["rased_bst_news_type_id"] != null)
            {
                defaultNewsType = long.Parse(row["rased_bst_news_type_id"].ToString());
            }
            else
            {
                defaultNewsType = null;
            }
            return true;
        }
        catch (Exception ex)
        {
            logger.LogError("Read general system settings error, ex: " + ex.Message);
            defualtNewsType = null;
            defualtNewsSource = null;
            return false;
        }
        finally
        {
            if (conn.State == ConnectionState.Open)
            {
                conn.Close();
            }
        }

        defualtNewsType = null;
        defualtNewsSource = null;
        return false;
    }
}
