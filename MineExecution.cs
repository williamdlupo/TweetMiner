using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using TweetMiner.Models;

namespace TweetMiner
{
    public static class MineExecution
    {
        static HttpClient httpClient = new HttpClient();
        static ILogger log;

        [FunctionName("RunTweetMiner")]
        public static async Task Run([TimerTrigger("* * 0 * * *")] TimerInfo myTimer, ILogger logger)
        {
            log = logger;
            log.LogInformation($"Starting tweet miner at: {DateTime.Now}");

            List<Entity> entities = await GetEntities();
            List<Tweet> tweets = await DailyTweets(entities);
            await StoreTweets(tweets);

            log.LogInformation($"Tweet miner completed!");
        }

        private static async Task<List<Entity>> GetEntities()
        {
            log.LogInformation("Gathering entities from database...");

            string connectionString = Environment.GetEnvironmentVariable("sqlConnection");
            using SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();

            string sqlError = "";
            List<Entity> entityResults = new List<Entity>();

            string selectAllStatement = "select * from Entity";
            using SqlCommand cmd = new SqlCommand(selectAllStatement, conn);
            try
            {
                using SqlDataReader sqlReader = await cmd.ExecuteReaderAsync();
                if (sqlReader.HasRows)
                {
                    while (await sqlReader.ReadAsync())
                    {
                        entityResults.Add(new Entity
                        {
                            Id = sqlReader.GetInt32(0),
                            Name = sqlReader.GetString(1),
                            State = sqlReader.GetString(2),
                            Handle = sqlReader.GetString(3)[1..],
                            Office = sqlReader.GetString(4)
                        });
                    }
                }

                else log.LogInformation("Entity table is empty");

                await sqlReader.CloseAsync();
            }
            catch (Exception e)
            {
                log.LogError(e.Message.ToString());
                sqlError = e.Message.ToString();
            }

            if (!string.IsNullOrEmpty(sqlError))
            {
                log.LogError(sqlError);
            }

            log.LogInformation($"{entityResults.Count} entities found");
            return entityResults;
        }

        private static async Task<List<Tweet>> DailyTweets(List<Entity> entities)
        {
            log.LogInformation("Gathering tweets...");

            string sevenDayEndpoint = Environment.GetEnvironmentVariable("sevenDayEndpoint");
            string bearerToken = Environment.GetEnvironmentVariable("twitterBearerToken");

            string yesterday = DateTime.Today.AddDays(-1).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ");

            List<Tweet> minedTweets = new List<Tweet>();

            // Mine tweets from previous day for all entites
            foreach (var entity in entities)
            {
                string oneDayUrl = $"{sevenDayEndpoint}?query=from:{entity.Handle}&max_results=100&start_time={yesterday}&tweet.fields=created_at,author_id,public_metrics";
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);

                var responseMessage = await httpClient.GetAsync(oneDayUrl);
                if (int.Parse(responseMessage.Headers.GetValues("x-rate-limit-remaining").FirstOrDefault()) == 0)
                {
                    log.LogInformation("Rate limit reached, sleeping for 15 minutes...");
                    Thread.Sleep(900000);
                    log.LogInformation("Resuming!");
                }

                var jsonToObject = new List<Dictionary<string, object>>();
                using (HttpContent content = responseMessage.Content)
                {
                    var json = content.ReadAsStringAsync().Result;
                    var jObject = JObject.Parse(json);

                    if (!jObject.ContainsKey("data")) continue;
                    jsonToObject = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(jObject["data"].ToString());
                }

                foreach (var tweetJson in jsonToObject)
                {
                    JObject publicMetricsJobj = (JObject)tweetJson.GetValueOrDefault("public_metrics");
                    Dictionary<string, int> publicMetrics = JsonConvert.DeserializeObject<Dictionary<string, int>>(publicMetricsJobj.ToString());

                    minedTweets.Add(new Tweet
                    {
                        EntityId = entity.Id,
                        TweetId = (string)tweetJson.GetValueOrDefault("id"),
                        Text = ((string)tweetJson.GetValueOrDefault("text")).Replace("'", ""),
                        CreatedAt = ((DateTime)tweetJson.GetValueOrDefault("created_at")).ToString(),
                        RetweetCount = publicMetrics.GetValueOrDefault("retweet_count"),
                        ReplyCount = publicMetrics.GetValueOrDefault("reply_count"),
                        LikeCount = publicMetrics.GetValueOrDefault("like_count"),
                        QuoteCount = publicMetrics.GetValueOrDefault("quote_count"),
                    });
                }
            }
            log.LogInformation($"{minedTweets.Count} found from yesterday.");

            return minedTweets;
        }

        private static async Task<List<Tweet>> MineTweets(List<Entity> entities)
        {
            log.LogInformation("Gathering tweets...");

            string sevenDayEndpoint = Environment.GetEnvironmentVariable("sevenDayEndpoint");
            string thirtyDayEndpoint = Environment.GetEnvironmentVariable("thirtyDayEndpoint");
            string fullArchiveEndpoint = Environment.GetEnvironmentVariable("fullArchiveEndpoint");
            string bearerToken = Environment.GetEnvironmentVariable("twitterBearerToken");

            List<Entity> senators = entities.Where(x => x.Office.ToLower().Equals("senator")).ToList();
            List<Entity> representatives = entities.Where(x => !x.Office.ToLower().Equals("senator")).ToList();

            List<Tweet> minedTweets = new List<Tweet>();

            // Mine tweets from previous 7 days for all entites
            foreach (var entity in entities)
            {
                string sevenDayUrl = $"{sevenDayEndpoint}?query=from:{entity.Handle}&max_results=100&tweet.fields=created_at,author_id,public_metrics";
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);

                var responseMessage = await httpClient.GetAsync(sevenDayUrl);
                if (int.Parse(responseMessage.Headers.GetValues("x-rate-limit-remaining").FirstOrDefault()) == 0)
                {
                    log.LogInformation("Rate limit reached, sleeping for 15 minutes...");
                    Thread.Sleep(900000);
                    log.LogInformation("Resuming!");
                }

                var jsonToObject = new List<Dictionary<string, object>>();
                using (HttpContent content = responseMessage.Content)
                {
                    var json = content.ReadAsStringAsync().Result;
                    var jObject = JObject.Parse(json);

                    if (!jObject.ContainsKey("data")) continue;
                    jsonToObject = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(jObject["data"].ToString());
                }

                foreach (var tweetJson in jsonToObject)
                {
                    JObject publicMetricsJobj = (JObject)tweetJson.GetValueOrDefault("public_metrics");
                    Dictionary<string, int> publicMetrics = JsonConvert.DeserializeObject<Dictionary<string, int>>(publicMetricsJobj.ToString());

                    minedTweets.Add(new Tweet
                    {
                        EntityId = entity.Id,
                        TweetId = (string)tweetJson.GetValueOrDefault("id"),
                        Text = ((string)tweetJson.GetValueOrDefault("text")).Replace("'",""),
                        CreatedAt = ((DateTime)tweetJson.GetValueOrDefault("created_at")).ToString(),
                        RetweetCount = publicMetrics.GetValueOrDefault("retweet_count"),
                        ReplyCount = publicMetrics.GetValueOrDefault("reply_count"),
                        LikeCount = publicMetrics.GetValueOrDefault("like_count"),
                        QuoteCount = publicMetrics.GetValueOrDefault("quote_count"),
                    });
                }
            }
            int sevenDayCount = minedTweets.Count;
            log.LogInformation($"{sevenDayCount} found from 7 day api");


            // Mine tweets from 30 days for all senators then representatives until rate limit reached
            foreach (var senator in senators)
            {
                string thirtyDayUrl = $"{thirtyDayEndpoint}dev.json";
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);

                string next = "";
                do
                {
                    var queryParamters =
                        new
                        {
                            query = $"from:{senator.Handle}",
                            maxResults = "100",
                            toDate = DateTime.Now.AddDays(-7).ToUniversalTime().ToString("yyyyMMddHHmm")
                        };
                    var queryWithNext =
                          new
                          {
                              query = $"from:{senator.Handle}",
                              maxResults = "100",
                              toDate = DateTime.Now.AddDays(-7).ToUniversalTime().ToString("yyyyMMddHHmm"),
                              next = next
                          };
                    HttpResponseMessage responseMessage = new HttpResponseMessage();

                    if (next.Equals("")) responseMessage = await httpClient.PostAsJsonAsync(thirtyDayUrl, queryParamters);
                    else responseMessage = await httpClient.PostAsJsonAsync(thirtyDayUrl, queryWithNext);

                    var jsonToObject = new List<Dictionary<string, object>>();
                    using (HttpContent content = responseMessage.Content)
                    {
                        var json = content.ReadAsStringAsync().Result;
                        var jObject = JObject.Parse(json);

                        if (!jObject.ContainsKey("results")) continue;
                        if (jObject.ContainsKey("next")) next = jObject["next"].ToString();

                        jsonToObject = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(jObject["results"].ToString());
                    }

                    foreach (var tweetJson in jsonToObject)
                    {
                        if ((bool)tweetJson.GetValueOrDefault("truncated"))
                        {
                            JObject extendedDetails = (JObject)tweetJson.GetValueOrDefault("extended_tweet");
                            Dictionary<string, object> publicMetrics = JsonConvert.DeserializeObject<Dictionary<string, object>>(extendedDetails.ToString());

                            minedTweets.Add(new Tweet
                            {
                                EntityId = senator.Id,
                                TweetId = (string)tweetJson.GetValueOrDefault("id_str"),
                                Text = (string)publicMetrics.GetValueOrDefault("full_text"),
                                CreatedAt = (string)tweetJson.GetValueOrDefault("created_at"),
                                RetweetCount = (long)tweetJson.GetValueOrDefault("retweet_count"),
                                ReplyCount = (long)tweetJson.GetValueOrDefault("reply_count"),
                                LikeCount = (long)tweetJson.GetValueOrDefault("favorite_count"),
                                QuoteCount = (long)tweetJson.GetValueOrDefault("quote_count"),
                            });
                        }
                        else
                        {
                            minedTweets.Add(new Tweet
                            {
                                EntityId = senator.Id,
                                TweetId = (string)tweetJson.GetValueOrDefault("id_str"),
                                Text = (string)tweetJson.GetValueOrDefault("text"),
                                CreatedAt = (string)tweetJson.GetValueOrDefault("created_at"),
                                RetweetCount = (long)tweetJson.GetValueOrDefault("retweet_count"),
                                ReplyCount = (long)tweetJson.GetValueOrDefault("reply_count"),
                                LikeCount = (long)tweetJson.GetValueOrDefault("favorite_count"),
                                QuoteCount = (long)tweetJson.GetValueOrDefault("quote_count"),
                            });
                        }
                    }
                }
                while (!String.IsNullOrEmpty(next));
            }
            log.LogInformation($"{minedTweets.Count - sevenDayCount} found from 30 day api");

            // TODO: Mine tweets up to 2 years old for each entity

            return minedTweets;
        }

        private static async Task StoreTweets(List<Tweet> tweets)
        {
            log.LogInformation($"Storing tweets to database...");

            string connectionString = Environment.GetEnvironmentVariable("sqlConnection");
            using SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();

            foreach (var tweet in tweets)
            {
                string insertStatement = $"insert into Tweet (EntityId, TweetId, Text, Created_At, RetweetCount, ReplyCount, LikeCount, QuoteCount)" +
                    $"values ({tweet.EntityId}, '{tweet.TweetId}', '{tweet.Text}', '{tweet.CreatedAt}', {tweet.RetweetCount}, {tweet.ReplyCount}, {tweet.LikeCount}, {tweet.QuoteCount})";

                using SqlCommand cmd = new SqlCommand(insertStatement, conn);
                try
                {
                    await cmd.ExecuteNonQueryAsync();
                }
                catch (Exception e)
                {
                    log.LogError(e.Message.ToString());
                    continue
                }
            }
        }
    }
}
