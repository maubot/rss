# rss
A [maubot](https://github.com/maubot/maubot) that posts RSS feed updates to Matrix.

## Usage
Basic commands:

* `!rss subscribe <url>` - Subscribe the current room to a feed.
* `!rss unsubscribe <feed ID>` - Unsubscribe the current room from a feed.
* `!rss subscriptions` - List subscriptions (and feed IDs) in the current room.
  Also shows any active feed filters.
* `!rss notice <feed ID> [true/false]` - Set whether the bot should send new
  posts as `m.notice` (if false, they're sent as `m.text`).
* `!rss template <feed ID> [new template]` - Change the post template for a
  feed in the current room. If the new template is omitted, the bot replies
  with the current template.
* `!rss filter <feed ID> [new filter]` - Set a regex feed filter for a feed
  in the current room. If the filter is omitted, the filter is removed. If
  the filter is invalid, the bot replies that it is invalid.

### Templates
The default template is `New post in $feed_title: [$title]($link)`.

Templates are interpreted as markdown with some simple variable substitution.
The following variables are available:

* `$feed_url` - The URL that was used to subscribe to the feed.
* `$feed_link` - The home page of the feed.
* `$feed_title` - The title of the feed.
* `$feed_subtitle` - The subtitle of the feed.
* `$id` - The unique ID of the entry.
* `$date` - The date of the entry.
* `$title` - The title of the entry.
* `$summary` - The summary/description of the entry.
* `$link` - The link of the entry.

### Filters
There is no filter by default. For busy feeds, you can apply a regex filter on a per subscription basis.
The filter is applied to the entry title. Here are some examples:

* `linux|foss` - Include entry titles matching either of the words "linux" or "foss".
* `(?i)linux|foss` - Same as before but with an inline modifier making it case-insensitive.
* `^(?!RT).*$` - Exclude entry titles that begin with "RT" using a negative lookahead.
