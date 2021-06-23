import logging
import os
import re
import shutil
import subprocess
from typing import Any, Callable, Dict, List, Set
from django.contrib.auth.models import User

import orjson
import bson
from django.conf import settings
from django.forms.models import model_to_dict
from django.utils.timezone import now as timezone_now

from zerver.data_import.import_util import (
    SubscriberHandler,
    ZerverFieldsT,
    build_huddle,
    build_huddle_subscriptions,
    build_message,
    build_personal_subscriptions,
    build_realm,
    build_realm_emoji,
    build_recipients,
    build_stream,
    build_stream_subscriptions,
    build_user_profile,
    build_zerver_realm,
    create_converted_data_files,
    make_subscriber_map,
    make_user_messages,
)
from zerver.data_import.user_handler import UserHandler
from zerver.data_import.sequencer import NEXT_ID, IdMapper
from zerver.lib.emoji import name_to_codepoint
from zerver.lib.utils import process_list_in_batches
from zerver.models import Reaction, RealmEmoji, Recipient, UserProfile


def make_realm(realm_id: int, realm_subdomain: str, domain_name: str) -> ZerverFieldsT:
    # set correct realm details
    NOW = float(timezone_now().timestamp())

    zerver_realm = build_zerver_realm(realm_id, realm_subdomain, NOW, "RocketChat")
    realm = build_realm(zerver_realm, realm_id, domain_name)

    # We may override these later.
    realm["zerver_defaultstream"] = []

    return realm


def process_users(
    user_data_map: Dict[str, Dict[str, Any]],
    realm_id: int,
    domain_name: str,
    user_handler: UserHandler,
    user_id_mapper: IdMapper,
) -> ZerverFieldsT:

    for rc_userid in user_data_map:
        user_dict = user_data_map[rc_userid]
        user_dict["is_mirror_dummy"] = False

        if user_dict["type"] != "user":
            user_dict["is_mirror_dummy"] = True
            emails = user_dict.get("emails")
            if emails is None or len(emails) == 0:
                user_dict["emails"] = [
                    {"address": "{}-{}@{}".format(user_dict["username"], user_dict["type"], domain_name)}
                ]

        # TODO: Change this to use actual exported avatar
        avatar_source = "G"
        full_name = user_dict["name"]
        id = user_id_mapper.get(rc_userid)
        delivery_email = user_dict["emails"][0]["address"]
        email = user_dict["emails"][0]["address"]
        short_name = user_dict["username"]
        date_joined = int(timezone_now().timestamp())
        timezone = "UTC"

        role = UserProfile.ROLE_MEMBER
        if "admin" in user_dict["roles"]:
            role = UserProfile.ROLE_REALM_OWNER  # or ADMINISTRATOR??
        elif "guest" in user_dict["roles"]:
            role = UserProfile.ROLE_GUEST

        if user_dict["is_mirror_dummy"]:
            is_active = False
            is_mirror_dummy = True
        else:
            is_active = True
            is_mirror_dummy = False

        user = build_user_profile(
            avatar_source=avatar_source,
            date_joined=date_joined,
            delivery_email=delivery_email,
            email=email,
            full_name=full_name,
            id=id,
            is_active=is_active,
            role=role,
            is_mirror_dummy=is_mirror_dummy,
            realm_id=realm_id,
            short_name=short_name,
            timezone=timezone,
        )
        user_handler.add_user(user)


def convert_channel_data(
    roomid_room_map: List[ZerverFieldsT],
    teamid_team_map,
    stream_id_mapper: IdMapper,
    realm_id: int,
) -> List[ZerverFieldsT]:
    streams = []

    for room_id in roomid_room_map:
        channel_dict = roomid_room_map[room_id]

        now = int(timezone_now().timestamp())
        stream_id = stream_id_mapper.get(room_id)
        invite_only = channel_dict["t"] == "p"

        stream_name = channel_dict["name"]
        stream_desc = channel_dict.get("description", "")
        if "teamId" in channel_dict:
            if channel_dict.get("teamMain") is True:
                stream_name = "[TEAM] " + stream_name
            else:
                stream_desc = "[Team {} channel]. {}".format(teamid_team_map[channel_dict["teamId"]]["name"], stream_desc)

        stream = build_stream(
            date_created=now,
            realm_id=realm_id,
            name=stream_name,
            description=stream_desc,
            stream_id=stream_id,
            deactivated=False,
            invite_only=invite_only,
        )
        streams.append(stream)

    return streams


def convert_subscription_data(
    userid_user_map,
    dscid_dsc_map,
    zerver_stream,
    stream_id_mapper: IdMapper,
    user_id_mapper: IdMapper,
    subscriber_handler: SubscriberHandler
) -> None:
    stream_members_map: Dict[str, Set[int]] = {}

    for user_id in userid_user_map:
        user_dict = userid_user_map[user_id]

        if "__rooms" not in user_dict:
            continue

        for channel in user_dict["__rooms"]:
            if channel in dscid_dsc_map:
                # Ignore discussion rooms
                continue
            stream_id = stream_id_mapper.get(channel)
            if stream_id not in stream_members_map:
                stream_members_map[stream_id] = set()
            stream_members_map[stream_id].add(user_id_mapper.get(user_id))

    for stream in zerver_stream:
        if stream["id"] in stream_members_map:
            users = stream_members_map[stream["id"]]
        else:
            users = set()
        subscriber_handler.set_info(
            users=users,
            stream_id=stream["id"]
        )


def process_raw_message_batch(
    realm_id: int,
    raw_messages: List[Dict[str, Any]],
    subscriber_map: Dict[int, Set[int]],
    user_handler: UserHandler,
    is_pm_data: bool,
    output_dir: str,
    # zerver_realmemoji: List[Dict[str, Any]],
    total_reactions: List[Dict[str, Any]],
) -> None:
    def fix_mentions(content: str, mention_user_ids: Set[int]) -> str:
        for user_id in mention_user_ids:
            user = user_handler.get_user(user_id=user_id)
            rc_mention = "@{short_name}".format(**user)
            zulip_mention = "@**{full_name}**".format(**user)
            content = content.replace(rc_mention, zulip_mention)

        content = content.replace("@all", "@**all**")
        # We don't have an equivalent for Mattermost's @here mention which mentions all users
        # active in the channel.
        content = content.replace("@here", "@**all**")
        return content

    mention_map: Dict[int, Set[int]] = {}
    zerver_message = []

    for raw_message in raw_messages:
        message_id = NEXT_ID("message")
        mention_user_ids = raw_message["mention_user_ids"]
        mention_map[message_id] = mention_user_ids

        content = fix_mentions(
            content=raw_message["content"],
            mention_user_ids=mention_user_ids,
        )

        if len(content) > 10000:  # nocoverage
            logging.info("skipping too-long message of length %s", len(content))
            continue

        date_sent = raw_message["date_sent"]
        sender_user_id = raw_message["sender_id"]
        recipient_id = raw_message["recipient_id"]

        rendered_content = None

        topic_name = raw_message["topic_name"]

        message = build_message(
            content=content,
            message_id=message_id,
            date_sent=date_sent,
            recipient_id=recipient_id,
            rendered_content=rendered_content,
            topic_name=topic_name,
            user_id=sender_user_id,
            has_attachment=False,
        )
        zerver_message.append(message)
        # build_reactions(
        #     realm_id,
        #     total_reactions,
        #     raw_message["reactions"],
        #     message_id,
        #     user_id_mapper,
        #     zerver_realmemoji,
        # )

    zerver_usermessage = make_user_messages(
        zerver_message=zerver_message,
        subscriber_map=subscriber_map,
        is_pm_data=is_pm_data,
        mention_map=mention_map,
    )

    message_json = dict(
        zerver_message=zerver_message,
        zerver_usermessage=zerver_usermessage,
    )

    dump_file_id = NEXT_ID("dump_file_id" + str(realm_id))
    message_file = f"/messages-{dump_file_id:06}.json"
    create_converted_data_files(message_json, output_dir, message_file)


def process_messages(
    realm_id: int,
    messages: List[Dict[str, Any]],
    subscriber_map: Dict[int, Set[int]],
    is_pm_data: bool,
    user_id_mapper: IdMapper,
    user_handler: UserHandler,
    user_id_to_recipient_id: Dict[str, str],
    stream_id_mapper: IdMapper,
    stream_id_to_recipient_id: Dict[str, str],
    directid_direct_map: Dict[str, ZerverFieldsT],
    dscid_dsc_map: Dict[str, ZerverFieldsT],
    # zerver_realmemoji: List[Dict[str, Any]],
    total_reactions: List[Dict[str, Any]],
    output_dir: str,
) -> None:
    def message_to_dict(message: Dict[str, Any]) -> Dict[str, Any]:
        sender_rc_id = message["u"]["_id"]
        sender_id = user_id_mapper.get(sender_rc_id)
        content = message["msg"]

        # if "reactions" in post_dict:
        #     reactions = post_dict["reactions"] or []
        # else:
        #     reactions = []
        reactions = []

        message_dict = dict(
            sender_id=sender_id,
            content=content,
            date_sent=int(message["ts"].timestamp()),
            reactions=reactions,
        )

        # Add recipient_id and topic to message_dict
        if is_pm_data:
            direct_channel_id = message["rid"]
            rc_member_ids = directid_direct_map[direct_channel_id]["uids"]
            if sender_rc_id == rc_member_ids[0]:
                zulip_member_id = user_id_mapper.get(rc_member_ids[1])
                message_dict["recipient_id"] = user_id_to_recipient_id[zulip_member_id]
            else:
                zulip_member_id = user_id_mapper.get(rc_member_ids[0])
                message_dict["recipient_id"] = user_id_to_recipient_id[zulip_member_id]
            # PMs don't have topics, but topic_name field is required in `build_message`.
            message_dict["topic_name"] = "Imported from rocketchat"
        elif message["rid"] in dscid_dsc_map:
            # Message is in a discussion
            dsc_channel = dscid_dsc_map[message["rid"]]
            parent_channel_id = dsc_channel["prid"]
            stream_id = stream_id_mapper.get(parent_channel_id)
            message_dict["recipient_id"] = stream_id_to_recipient_id[stream_id]
            message_dict["topic_name"] = "(Discussion) {}".format(dsc_channel["fname"])
        else:
            stream_id = stream_id_mapper.get(message["rid"])
            message_dict["recipient_id"] = stream_id_to_recipient_id[stream_id]
            message_dict["topic_name"] = "Main channel content (imported from rocketchat)"

        # Add mentions to message_dict
        mention_user_ids = set()
        for mention in message.get("mentions", []):
            mention_id = mention["_id"]
            if mention_id in ["all", "here"]:
                continue
            user_id = user_id_mapper.get(mention_id)
            mention_user_ids.add(user_id)
        message_dict["mention_user_ids"] = mention_user_ids

        return message_dict

    raw_messages = []
    for message in messages:
        if message.get("t") is not None:
            # Message contains user_joined, added_user, discussion_created,
            # etc. feeds.
            continue
        raw_messages.append(message_to_dict(message))

    def process_batch(lst: List[Dict[str, Any]]) -> None:
        process_raw_message_batch(
            realm_id=realm_id,
            raw_messages=lst,
            subscriber_map=subscriber_map,
            user_handler=user_handler,
            is_pm_data=is_pm_data,
            output_dir=output_dir,
            # zerver_realmemoji=zerver_realmemoji,
            total_reactions=total_reactions,
        )

    chunk_size = 1000

    process_list_in_batches(
        lst=raw_messages,
        chunk_size=chunk_size,
        process_batch=process_batch,
    )


def convert_data_to_json(rocketchat_data_dir: str, json_output_dir: str) -> None:
    for file_name in os.listdir(rocketchat_data_dir):
        if file_name.split(".")[1] == "bson":
            print(file_name)
            with open(os.path.join(rocketchat_data_dir, file_name), 'rb') as fcache:
                file_nn = file_name.split(".")[0]
                print(file_nn)
                users_json = bson.decode_all(fcache.read())
                create_converted_data_files(users_json, json_output_dir+"/", file_nn+".json")


def create_username_to_user_mapping(
    user_data_list: List[Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    username_to_user = {}
    for user in user_data_list:
        username_to_user[user["username"]] = user
    return username_to_user


def separate_channel_and_private_messages(
    messages,
    direct_room_list,
    channel_messages,
    private_messages: List[ZerverFieldsT]
) -> None:
    for message in messages:
        if message["rid"] in direct_room_list:
            private_messages.append(message)
        else:
            channel_messages.append(message)


def map_reciever_id_to_recipient_id(
    zerver_recipient,
    stream_id_to_recipient_id,
    user_id_to_recipient_id
) -> None:
    # reciever_id --> stream_id/user_id
    for recipient in zerver_recipient:
        if recipient["type"] == Recipient.STREAM:
            stream_id_to_recipient_id[recipient["type_id"]] = recipient["id"]
        elif recipient["type"] == Recipient.PERSONAL:
            user_id_to_recipient_id[recipient["type_id"]] = recipient["id"]


def categorize_channels_and_map_with_id(
    channel_data,
    roomid_room_map,
    teamid_team_map,
    dscid_dsc_map,
    directid_direct_map
) -> None:
    for channel in channel_data:
        if "prid" in channel:
            dscid_dsc_map[channel["_id"]] = channel
        elif channel["t"] == "d":
            directid_direct_map[channel["_id"]] = channel
        else:
            roomid_room_map[channel["_id"]] = channel
            if channel.get("teamMain") is True:
                teamid_team_map[channel["teamId"]] = channel


def map_userid_to_user(user_data_list: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    userid_user_map = {}
    for user in user_data_list:
        userid_user_map[user["_id"]] = user
    return userid_user_map


def rocketchat_data_to_dict(rocketchat_data_dir: str) -> Dict[str, Any]:
    rocketchat_data: Dict[str, Any] = {}
    rocketchat_data["user"] = []
    rocketchat_data["avatar"] = {"avatar": [], "file": [], "chunk": []}
    rocketchat_data["room"] = []
    rocketchat_data["message"] = []

    # Get user
    with open(os.path.join(rocketchat_data_dir, "users.bson"), 'rb') as fcache:
        rocketchat_data["user"] = bson.decode_all(fcache.read())

    # Get avatar
    with open(os.path.join(rocketchat_data_dir, "rocketchat_avatars.bson"), 'rb') as fcache:
        rocketchat_data["avatar"]["avatar"] = bson.decode_all(fcache.read())

    with open(os.path.join(rocketchat_data_dir, "rocketchat_avatars.chunks.bson"), 'rb') as fcache:
        rocketchat_data["avatar"]["chunk"] = bson.decode_all(fcache.read())

    with open(os.path.join(rocketchat_data_dir, "rocketchat_avatars.files.bson"), 'rb') as fcache:
        rocketchat_data["avatar"]["file"] = bson.decode_all(fcache.read())

    # Get room
    with open(os.path.join(rocketchat_data_dir, "rocketchat_room.bson"), 'rb') as fcache:
        rocketchat_data["room"] = bson.decode_all(fcache.read())

    # Get messages
    with open(os.path.join(rocketchat_data_dir, "rocketchat_message.bson"), 'rb') as fcache:
        rocketchat_data["message"] = bson.decode_all(fcache.read())



    return rocketchat_data


def do_convert_data(rocketchat_data_dir: str, output_dir: str) -> None:
    # Subdomain is set by the user while running the import command
    realm_subdomain = ""
    realm_id = 0
    domain_name = settings.EXTERNAL_HOST

    realm = make_realm(realm_id, realm_subdomain, domain_name)

    # Get all required exported data in a dictionary
    rocketchat_data = rocketchat_data_to_dict(rocketchat_data_dir)

    userid_user_map: Dict[str, Dict[str, Any]] = map_userid_to_user(rocketchat_data["user"])

    user_handler = UserHandler()
    subscriber_handler = SubscriberHandler()
    user_id_mapper = IdMapper()
    stream_id_mapper = IdMapper()

    process_users(
        user_data_map=userid_user_map,
        realm_id=realm_id,
        domain_name=domain_name,
        user_handler=user_handler,
        user_id_mapper=user_id_mapper,
    )

    roomid_room_map = {}
    teamid_team_map = {}
    dscid_dsc_map = {}
    directid_direct_map: Dict[str, Dict[str, Any]] = {}

    categorize_channels_and_map_with_id(
        rocketchat_data["room"],
        roomid_room_map,
        teamid_team_map,
        dscid_dsc_map,
        directid_direct_map
    )

    zerver_stream = convert_channel_data(
        roomid_room_map=roomid_room_map,
        teamid_team_map=teamid_team_map,
        stream_id_mapper=stream_id_mapper,
        realm_id=realm_id,
    )
    realm["zerver_stream"] = zerver_stream

    # Add subscription data to subscriber handler
    convert_subscription_data(
        userid_user_map=userid_user_map,
        dscid_dsc_map=dscid_dsc_map,
        zerver_stream=zerver_stream,
        stream_id_mapper=stream_id_mapper,
        user_id_mapper=user_id_mapper,
        subscriber_handler=subscriber_handler
    )

    all_users = user_handler.get_all_users()

    zerver_recipient = build_recipients(
        zerver_userprofile=all_users,
        zerver_stream=zerver_stream,
    )
    realm["zerver_recipient"] = zerver_recipient

    stream_subscriptions = build_stream_subscriptions(
        get_users=subscriber_handler.get_users,
        zerver_recipient=zerver_recipient,
        zerver_stream=zerver_stream,
    )

    personal_subscriptions = build_personal_subscriptions(
        zerver_recipient=zerver_recipient,
    )

    zerver_subscription = personal_subscriptions + stream_subscriptions
    realm["zerver_subscription"] = zerver_subscription

    # zerver_realmemoji = write_emoticon_data(
    #     realm_id=realm_id,
    #     custom_emoji_data=mattermost_data["emoji"],
    #     data_dir=mattermost_data_dir,
    #     output_dir=output_dir,
    # )
    # realm["zerver_realmemoji"] = zerver_realmemoji

    subscriber_map = make_subscriber_map(
        zerver_subscription=zerver_subscription,
    )

    stream_id_to_recipient_id = {}
    user_id_to_recipient_id = {}
    
    map_reciever_id_to_recipient_id(
        zerver_recipient,
        stream_id_to_recipient_id,
        user_id_to_recipient_id
    )

    channel_messages: List[ZerverFieldsT] = []
    private_messages: List[ZerverFieldsT] = []

    separate_channel_and_private_messages(
        rocketchat_data["message"],
        directid_direct_map.keys(),
        channel_messages,
        private_messages
    )

    total_reactions: List[Dict[str, Any]] = []

    # Process channel messages
    process_messages(
        realm_id=realm_id,
        messages=channel_messages,
        subscriber_map=subscriber_map,
        is_pm_data=False,
        user_id_mapper=user_id_mapper,
        user_handler=user_handler,
        user_id_to_recipient_id=user_id_to_recipient_id,
        stream_id_mapper=stream_id_mapper,
        stream_id_to_recipient_id=stream_id_to_recipient_id,
        directid_direct_map=directid_direct_map,
        dscid_dsc_map=dscid_dsc_map,
        # zerver_realmemoji=zerver_realmemoji,
        total_reactions=total_reactions,
        output_dir=output_dir,
    )
    # Process private messages
    process_messages(
        realm_id=realm_id,
        messages=private_messages,
        subscriber_map=subscriber_map,
        is_pm_data=True,
        user_id_mapper=user_id_mapper,
        user_handler=user_handler,
        user_id_to_recipient_id=user_id_to_recipient_id,
        stream_id_mapper=stream_id_mapper,
        stream_id_to_recipient_id=stream_id_to_recipient_id,
        directid_direct_map=directid_direct_map,
        dscid_dsc_map=dscid_dsc_map,
        # zerver_realmemoji=zerver_realmemoji,
        total_reactions=total_reactions,
        output_dir=output_dir,
    )
    realm["zerver_reaction"] = total_reactions
    realm["zerver_userprofile"] = user_handler.get_all_users()
    realm["sort_by_date"] = True

    create_converted_data_files(realm, output_dir, "/realm.json")
    # Mattermost currently doesn't support exporting avatars
    create_converted_data_files([], output_dir, "/avatars/records.json")
    # Mattermost currently doesn't support exporting uploads
    create_converted_data_files([], output_dir, "/uploads/records.json")

    # Mattermost currently doesn't support exporting attachments
    attachment: Dict[str, List[Any]] = {"zerver_attachment": []}
    create_converted_data_files(attachment, output_dir, "/attachment.json")

    logging.info("Start making tarball")
    subprocess.check_call(["tar", "-czf", output_dir + ".tar.gz", output_dir, "-P"])
    logging.info("Done making tarball")

    # convert_data_to_json(rocketchat_data_dir, rocketchat_data_dir)
