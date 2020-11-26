
namespace TweetMiner.Models
{
    public struct Tweet
    {
        public int Id { get; set; }
        public int EntityId { get; set; }
        public string TweetId { get; set; }
        public string Text { get; set; }
        public string CreatedAt { get; set; }
        public long RetweetCount { get; set; }
        public long ReplyCount { get; set; }
        public long LikeCount { get; set; }
        public long QuoteCount { get; set; }
    }
}
