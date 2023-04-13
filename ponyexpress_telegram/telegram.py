"""Scraping Telegram Connector for ponyexpress

ToDo:
    - should return a one-rowed DataFrame with just the name if no data is
      present, thus, the calling program can detect an fault/non-existing data.
"""

from functools import reduce
from typing import List, Optional, Tuple, Dict, Any, Callable

import pandas as pd
import requests
from loguru import logger as log
from lxml import html, etree

message_paths = {
    # pylint: disable=C0301
    "post_id": "../@data-post",
    "views": ".//span[@class='tgme_widget_message_views']/descendant-or-self::*/text()",
    "datetime": ".//time/@datetime",
    "user": ".//a[@class='tgme_widget_message_owner_name']/descendant-or-self::*/text()",
    "from_author": ".//span[@class='tgme_widget_message_from_author']/descendant-or-self::*/text()",
    "text": "./div[contains(@class, 'tgme_widget_message_text')]/descendant-or-self::*/text()",
    "link": ".//div[contains(@class, 'tgme_widget_message_text')]//a/@href",
    "reply_to_user": ".//a[@class='tgme_widget_message_reply']//span[@class='tgme_widget_message_author_name'/descendant-or-self::*/text()]",
    "reply_to_text": ".//a[@class='tgme_widget_message_reply']//div[@class='tgme_widget_message_text']/descendant-or-self::*/text()",
    "reply_to_link": ".//a[@class='tgme_widget_message_reply']/@href",
    "image_url": "./a[contains(@class, 'tgme_widget_message_photo_wrap')]/@style",
    "forwarded_message_url": ".//a[@class='tgme_widget_message_forwarded_from_name']/@href",
    "forwarded_message_user": ".//a[@class='tgme_widget_message_forwarded_from_name']/descendant-or-self::*/text()",
    #       "poll_question" : ".//div[@class='tgme_widget_message_poll_question']/descendant-or-self::*/text()",
    #       "poll_options_text" : ".//div[@class='tgme_widget_message_poll_option_value']/descendant-or-self::*/text()",
    #       "poll_options_percent" : ".//div[@class='tgme_widget_message_poll_option_percent']/descendant-or-self::*/text()",
    "video_url": ".//video[contains(@class, 'tgme_widget_message_video')]/@src",
    "video_duration": ".//time[contains(@class, 'message_video_duration')]/descendant-or-self::*/text()",
}

user_paths = {
    # pylint: disable=C0301
    "name": '//div[@class="tgme_channel_info_header_username"]/a/text()',
    "fullname": '//div[@class="tgme_channel_info_header_title"]/descendant-or-self::*/text()',
    "url": '//div[@class="tgme_channel_info_header_username"]/a/@href',
    "description": '//div[@class="tgme_channel_info_description"]/descendant-or-self::*/text()',
    "subscriber_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "subscribers"]/preceding-sibling::span/text()',
    "photos_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "photos"]/preceding-sibling::span/text()',
    "videos_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "videos"]/preceding-sibling::span/text()',
    "files_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "files"]/preceding-sibling::span/text()',
    "links_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "links"]/preceding-sibling::span/text()',
}


def parse_tg_number_string(num: str) -> float:
    """parses the telegram number format"""
    if len(num) > 0:
        if num.isdigit():
            return float(num)
        return float(num.replace("K", "E+03").replace("M", "E+06"))
    return 0.0


def extract_from(tree, xpaths):
    """extracts data from a lxml tree based on a dictionary of xpaths"""
    return {name: tree.xpath(xpath) for name, xpath in xpaths.items()}


def cleanup(data_dict: Dict[str, Any], rules: Dict[Callable, Callable]) -> Dict[str, Any]:
    """Rule-based data cleaning.

    Args:
        data_dict: the data to be cleaned
        rules: the rules to be applied, whereas a ruleset is a dictionary with predicates as keys
            and cleaning functions as values. The predicate is a function that takes the key and
            value of the data_dict and returns a boolean. If the predicate returns True, the
            corresponding cleaning function is applied to the value.
    Returns:
        Dict[str, Any] : the cleaned data
    """
    return {
        key: reduce(lambda _value, ruleset: ruleset[1](_value) if ruleset[0](key, _value) is
        True else _value, rules.items(), value)
        for key, value in data_dict.items()
    }


def get_messages(page: etree.ElementTree) -> pd.DataFrame:
    """get Telegram messages from a HTML document

    Args:
      page: the parsed HTML document

    Returns:
      the parsed messages, where the columns are based on the
      keys of the messages_paths-dictionary as a ``pd.DataFrame``.
    """

    base_xpath = "//div[@class='tgme_widget_message_bubble']"
    messages_html = page.xpath(base_xpath)
    rules = {
        lambda key, value: key == "text": lambda x: "\n".join(x),
        lambda key, value: key == "datetime": lambda x: pd.to_datetime(x[0]),
        lambda key, value: "count" in key: lambda x: parse_tg_number_string(x[0]),
        lambda key, value: key == "views": lambda x: parse_tg_number_string(x[0]),
        lambda key, value: isinstance(value, list): lambda x: x[0] if len(x) > 0 else None,
        lambda key, value: True: lambda x: x if x != "" else None,
    }
    try:
        data = [extract_from(_, message_paths) for _ in messages_html]
    except ValueError:
        # If this code is executed, we could not obtain data from the TG-page
        return pd.DataFrame()

    data = [cleanup(_, rules) for _ in data]

    data = pd.DataFrame(data)
    data[["handle", "post_number"]] = data.post_id.str.split("/", n=1, expand=True)
    return data


def get_user(page) -> pd.DataFrame:
    """get Telegram user data from an HTML document

    Args:
      page: the parsed HTML document

    Returns:
      The parsed messages in a ``pd.DataFrame``, where the columns are based on the
       keys of the user_paths-dictionary.
    """
    rules = {
        lambda key, value: isinstance(value, list): lambda x: x[0] if len(x) > 0 else None,
        lambda key, value: key == "name": lambda x: x.replace("@", ""),
        lambda key, value: "count" in key: lambda x: parse_tg_number_string(x),
        lambda key, value: True: lambda x: x if x != "" else None,
    }

    try:
        data = extract_from(page, user_paths)
        data = cleanup(data, rules)

        log.debug(f"Extracted user data: {data}")

        data = pd.DataFrame([data])
    except ValueError:
        # If this code is executed, we could not obtain data from the TG-page
        return pd.DataFrame()
    except AttributeError:
        return pd.DataFrame()
    # post process entries
    return data


def get_node(node_name: str) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    visit_url = f"https://t.me/s/{node_name}"

    resp = requests.get(visit_url)

    log.debug(f"Visited node {node_name} with status: {resp.status_code}")

    if resp.status_code != 200:
        log.warning(f"{visit_url} did not succeed with status {resp.status_code}")

        return pd.DataFrame(), pd.DataFrame([{"name": node_name, "handle": node_name}])

    else:
        html_source = html.fromstring(resp.content)
        messages = get_messages(html_source)
        nodes = get_user(html_source)

        return messages, nodes


def telegram_connector(node_names: List[str], configuration: Dict[str, Any]) -> Tuple[pd.DataFrame,
pd.DataFrame]:
    """for a list of handles scrape their public telegram channels

    That is, if there is a public channel present for the specified handles.

    Args:
      node_names:  list of handles to scrape

    Returns:
     edges, nodes in a Tuple[pd.DataFrame, pd.DataFrame].
    """
    def _reduce_returns(
            carry: Tuple[pd.DataFrame, pd.DataFrame],
            value: Tuple[pd.DataFrame, pd.DataFrame],
    ):
        return pd.concat([carry[0], value[0]]), pd.concat([carry[1], value[1]])

    _ret_ = [get_node(_) for _ in node_names]
    _ret_ = [_ for _ in _ret_ if _ is not None]
    if len(_ret_) <= 0:
        return pd.DataFrame(), pd.DataFrame()
    if configuration.get("prepare_edges", False):
        if len(_ret_[0]) > 0:
            edges = _ret_[0].loc[
                    ~_ret_[0]["forwarded_message_url"].isnull(), :
                    ].copy()

            if len(edges) > 0:
                edges.loc[:, "source"] = edges.loc[:, "handle"]
                edges.loc[:, "target"] = edges.forwarded_message_url.str.extract(
                    r"([\w_]+)(?=\/\d+)"
                )
                edges.loc[:, "type"] = "forward"
                _ret_[0] = edges

    return reduce(_reduce_returns, _ret_)
