from praw import Reddit
import praw
import sys
import pandas as pd
import numpy as np
from utils.constants import POST_FIELDS


def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret = client_secret,
            user_agent=user_agent
        )
        print("Connected to reddit!")
        return reddit
    except Exception as e:
        print(e)
        sys.ext(1)

#{'comment_limit': 2048, 'comment_sort': 'confidence', '_reddit': <praw.reddit.Reddit object at 0x7fa4c6e58670>, 'approved_at_utc': None, 'subreddit': Subreddit(display_name='dataengineering'), 'selftext': "What is the future of Snowflake as a product in your opinion?\n\nFrom my limited understanding it is competing in the space with Google, Microsoft it appears and I wonder how Snowflake will stack up in the long term against its competitors.\n\nI guess my wonder is, even if Microsoft and Google might not have their competing products figured out, they eventually could by throwing money at it and out compete Snowflake?\n\nWhat are your thoughts?\n\nWhat might be Snowflake's exit strategy?", 'author_fullname': 't2_19qm92zzgg', 'saved': False, 'mod_reason_title': None, 'gilded': 0, 'clicked': False, 'title': 'What is the future of Snowflake as a product in your opinion?', 'link_flair_richtext': [], 'subreddit_name_prefixed': 'r/dataengineering', 'hidden': False, 'pwls': 6, 'link_flair_css_class': '', 'downs': 0, 'thumbnail_height': None, 'top_awarded_type': None, 'hide_score': False, 'name': 't3_1fvinjr', 'quarantine': False, 'link_flair_text_color': 'light', 'upvote_ratio': 0.94, 'author_flair_background_color': None, 'subreddit_type': 'public', 'ups': 76, 'total_awards_received': 0, 'media_embed': {}, 'thumbnail_width': None, 'author_flair_template_id': None, 'is_original_content': False, 'user_reports': [], 'secure_media': None, 'is_reddit_media_domain': False, 'is_meta': False, 'category': None, 'secure_media_embed': {}, 'link_flair_text': 'Discussion', 'can_mod_post': False, 'score': 76, 'approved_by': None, 'is_created_from_ads_ui': False, 'author_premium': False, 'thumbnail': 'self', 'edited': 1727992008.0, 'author_flair_css_class': None, 'author_flair_richtext': [], 'gildings': {}, 'content_categories': None, 'is_self': True, 'mod_note': None, 'created': 1727990528.0, 'link_flair_type': 'text', 'wls': 6, 'removed_by_category': None, 'banned_by': None, 'author_flair_type': 'text', 'domain': 'self.dataengineering', 'allow_live_comments': False, 'selftext_html': '<!-- SC_OFF --><div class="md"><p>What is the future of Snowflake as a product in your opinion?</p>\n\n<p>From my limited understanding it is competing in the space with Google, Microsoft it appears and I wonder how Snowflake will stack up in the long term against its competitors.</p>\n\n<p>I guess my wonder is, even if Microsoft and Google might not have their competing products figured out, they eventually could by throwing money at it and out compete Snowflake?</p>\n\n<p>What are your thoughts?</p>\n\n<p>What might be Snowflake&#39;s exit strategy?</p>\n</div><!-- SC_ON -->', 'likes': None, 'suggested_sort': None, 'banned_at_utc': None, 'view_count': None, 'archived': False, 'no_follow': False, 'is_crosspostable': False, 'pinned': False, 'over_18': False, 'all_awardings': [], 'awarders': [], 'media_only': False, 'link_flair_template_id': '92b74b58-aaca-11eb-b160-0e6181e3773f', 'can_gild': False, 'spoiler': False, 'locked': False, 'author_flair_text': None, 'treatment_tags': [], 'visited': False, 'removed_by': None, 'num_reports': None, 'distinguished': None, 'subreddit_id': 't5_36en4', 'author_is_blocked': False, 'mod_reason_by': None, 'removal_reason': None, 'link_flair_background_color': '#ff4500', 'id': '1fvinjr', 'is_robot_indexable': True, 'report_reasons': None, 'author': Redditor(name='RingaDingDingggg'), 'discussion_type': None, 'num_comments': 70, 'send_replies': False, 'whitelist_status': 'all_ads', 'contest_mode': False, 'mod_reports': [], 'author_patreon_flair': False, 'author_flair_text_color': None, 'permalink': '/r/dataengineering/comments/1fvinjr/what_is_the_future_of_snowflake_as_a_product_in/', 'parent_whitelist_status': 'all_ads', 'stickied': False, 'url': 'https://www.reddit.com/r/dataengineering/comments/1fvinjr/what_is_the_future_of_snowflake_as_a_product_in/', 'subreddit_subscribers': 218172, 'created_utc': 1727990528.0, 'num_crossposts': 0, 'media': None, 'is_video': False, '_fetched': False, '_additional_fetch_params': {}, '_comments_by_id': {}}


def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter:str, limit=None):

    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    #print(posts)

    post_lists = []

    for post in posts:
        post_dict = vars(post)
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)
    
    return post_lists

def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['title'] = post_df['title'].astype(str)

    return post_df


def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)

