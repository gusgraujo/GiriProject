class Tweet:
    def __init__(self, created_at, full_text, id, favorite_count, retweet_count, source, lang, user):
        self.created_at = created_at
        self.full_text = full_text
        self.id = id
        self.favorite_count = favorite_count
        self.retweet_count = retweet_count
        self.source = source
        self.lang = lang
        self.user = user

    def __str__(self):
        return f"Created at: {self.created_at}\nText: {self.full_text}\nID: {self.id}\nLikes: {self.favorite_count}\nRetweets: {self.retweet_count}\nSource: {self.source}\nLanguage: {self.lang}\nAuthor: {self.user}"
