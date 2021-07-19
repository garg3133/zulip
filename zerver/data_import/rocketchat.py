import logging
import os
import subprocess
from typing import Any, Dict, List, Set

import bson
from django.conf import settings
from django.forms.models import model_to_dict
from django.utils.timezone import now as timezone_now

from zerver.data_import.import_util import (
    SubscriberHandler,
    ZerverFieldsT,
    build_message,
    build_personal_subscriptions,
    build_realm,
    build_recipients,
    build_stream,
    build_stream_subscriptions,
    build_user_profile,
    build_zerver_realm,
    create_converted_data_files,
    make_subscriber_map,
    make_user_messages,
)
from zerver.data_import.sequencer import NEXT_ID, IdMapper
from zerver.data_import.user_handler import UserHandler
from zerver.lib.emoji import name_to_codepoint
from zerver.lib.utils import process_list_in_batches
from zerver.models import Reaction, Recipient, UserProfile


def make_realm(realm_id: int, realm_subdomain: str, domain_name: str) -> ZerverFieldsT:
    NOW = float(timezone_now().timestamp())

    zerver_realm = build_zerver_realm(realm_id, realm_subdomain, NOW, "Rocket.Chat")
    realm = build_realm(zerver_realm, realm_id, domain_name)

    # We may override these later.
    realm["zerver_defaultstream"] = []

    return realm


def process_users(
    user_id_to_user_map: Dict[str, Dict[str, Any]],
    realm_id: int,
    domain_name: str,
    user_handler: UserHandler,
    user_id_mapper: IdMapper,
) -> None:
    for rc_user_id in user_id_to_user_map:
        user_dict = user_id_to_user_map[rc_user_id]
        is_mirror_dummy = False

        # Rocket.Chat has three user types:
        # "user": This is a regular user of the system.
        # "bot": A special user types for bots.
        # "unknown": This usually represents a livechat guest.
        if user_dict["type"] != "user":
            is_mirror_dummy = True
            if not user_dict.get("emails"):
                user_dict["emails"] = [
                    {
                        "address": "{}-{}@{}".format(
                            user_dict["username"], user_dict["type"], domain_name
                        )
                    }
                ]

        # TODO: Change this to use actual exported avatar
        avatar_source = "G"
        full_name = user_dict["name"]
        id = user_id_mapper.get(rc_user_id)
        delivery_email = user_dict["emails"][0]["address"]
        email = user_dict["emails"][0]["address"]
        short_name = user_dict["username"]
        date_joined = int(timezone_now().timestamp())
        timezone = "UTC"

        role = UserProfile.ROLE_MEMBER
        if "admin" in user_dict["roles"]:
            role = UserProfile.ROLE_REALM_OWNER
        elif "guest" in user_dict["roles"]:
            role = UserProfile.ROLE_GUEST

        is_active = not is_mirror_dummy

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
    room_id_to_room_map: Dict[str, Dict[str, Any]],
    team_id_to_team_map: Dict[str, Dict[str, Any]],
    stream_id_mapper: IdMapper,
    realm_id: int,
) -> List[ZerverFieldsT]:
    streams = []

    for rc_room_id in room_id_to_room_map:
        channel_dict = room_id_to_room_map[rc_room_id]

        now = int(timezone_now().timestamp())
        stream_id = stream_id_mapper.get(rc_room_id)
        invite_only = channel_dict["t"] == "p"

        stream_name = channel_dict["name"]
        stream_desc = channel_dict.get("description", "")
        if channel_dict.get("teamId"):
            if channel_dict.get("teamMain") is True:
                stream_name = "[TEAM] " + stream_name
            else:
                stream_desc = "[Team {} channel]. {}".format(
                    team_id_to_team_map[channel_dict["teamId"]]["name"], stream_desc
                )

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
    user_id_to_user_map: Dict[str, Dict[str, Any]],
    dsc_id_to_dsc_map: Dict[str, Dict[str, Any]],
    zerver_stream: List[ZerverFieldsT],
    stream_id_mapper: IdMapper,
    user_id_mapper: IdMapper,
    subscriber_handler: SubscriberHandler,
) -> None:
    stream_members_map: Dict[int, Set[int]] = {}

    for rc_user_id in user_id_to_user_map:
        user_dict = user_id_to_user_map[rc_user_id]

        if not user_dict.get("__rooms"):
            continue

        for channel in user_dict["__rooms"]:
            if channel in dsc_id_to_dsc_map:
                # Ignore discussion rooms as these are not
                # imported as streams, but topics.
                continue
            stream_id = stream_id_mapper.get(channel)
            if stream_id not in stream_members_map:
                stream_members_map[stream_id] = set()
            stream_members_map[stream_id].add(user_id_mapper.get(rc_user_id))

    for stream in zerver_stream:
        if stream["id"] in stream_members_map:
            users = stream_members_map[stream["id"]]
        else:
            users = set()
        subscriber_handler.set_info(users=users, stream_id=stream["id"])


def build_reactions(
    total_reactions: List[ZerverFieldsT],
    reactions: List[Dict[str, Any]],
    message_id: int,
    # zerver_realmemoji: List[ZerverFieldsT],
) -> None:
    # For the Unicode emoji codes, we use equivalent of
    # function 'emoji_name_to_emoji_code' in 'zerver/lib/emoji' here
    for reaction in reactions:
        emoji_name = reaction["name"]
        user_id = reaction["user_id"]
        # Check in Unicode emoji
        if emoji_name in name_to_codepoint:
            emoji_code = name_to_codepoint[emoji_name]
            reaction_type = Reaction.UNICODE_EMOJI
        else:  # nocoverage
            continue

        reaction_id = NEXT_ID("reaction")
        reaction = Reaction(
            id=reaction_id,
            emoji_code=emoji_code,
            emoji_name=emoji_name,
            reaction_type=reaction_type,
        )

        reaction_dict = model_to_dict(reaction, exclude=["message", "user_profile"])
        reaction_dict["message"] = message_id
        reaction_dict["user_profile"] = user_id
        total_reactions.append(reaction_dict)


def process_raw_message_batch(
    realm_id: int,
    raw_messages: List[Dict[str, Any]],
    subscriber_map: Dict[int, Set[int]],
    user_handler: UserHandler,
    is_pm_data: bool,
    output_dir: str,
    # zerver_realmemoji: List[Dict[str, Any]],
    total_reactions: List[ZerverFieldsT],
) -> None:
    def fix_mentions(content: str, mention_user_ids: Set[int]) -> str:
        for user_id in mention_user_ids:
            user = user_handler.get_user(user_id=user_id)
            rc_mention = "@{short_name}".format(**user)
            zulip_mention = "@**{full_name}**".format(**user)
            content = content.replace(rc_mention, zulip_mention)

        content = content.replace("@all", "@**all**")
        # We don't have an equivalent for Rocket.Chat's @here mention
        # which mentions all users active in the channel.
        content = content.replace("@here", "@**all**")
        return content

    mention_map: Dict[int, Set[int]] = {}
    zerver_message: List[ZerverFieldsT] = []

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
        build_reactions(
            total_reactions=total_reactions,
            reactions=raw_message["reactions"],
            message_id=message_id,
            # zerver_realmemoji,
        )

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
    username_to_user_id_map: Dict[str, str],
    user_id_mapper: IdMapper,
    user_handler: UserHandler,
    user_id_to_recipient_id: Dict[int, int],
    stream_id_mapper: IdMapper,
    stream_id_to_recipient_id: Dict[int, int],
    direct_id_to_direct_map: Dict[str, Dict[str, Any]],
    dsc_id_to_dsc_map: Dict[str, Dict[str, Any]],
    # zerver_realmemoji: List[Dict[str, Any]],
    total_reactions: List[ZerverFieldsT],
    output_dir: str,
) -> None:
    def list_reactions(reactions: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        # List of dictionaries of form:
        # {"name": "smile", "user_id": 2}
        reactions_list: List[Dict[str, Any]] = []
        for react_code in reactions:
            name = react_code.split(":")[1]
            usernames = reactions[react_code]["usernames"]

            for username in usernames:
                rc_user_id = username_to_user_id_map[username]
                user_id = user_id_mapper.get(rc_user_id)
                reactions_list.append({"name": name, "user_id": user_id})

        return reactions_list

    def message_to_dict(message: Dict[str, Any]) -> Dict[str, Any]:
        rc_sender_id = message["u"]["_id"]
        sender_id = user_id_mapper.get(rc_sender_id)
        content = message["msg"]

        if message.get("reactions"):
            reactions = list_reactions(message["reactions"])
        else:
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
            rc_member_ids = direct_id_to_direct_map[direct_channel_id]["uids"]
            if rc_sender_id == rc_member_ids[0]:
                zulip_member_id = user_id_mapper.get(rc_member_ids[1])
                message_dict["recipient_id"] = user_id_to_recipient_id[zulip_member_id]
            else:
                zulip_member_id = user_id_mapper.get(rc_member_ids[0])
                message_dict["recipient_id"] = user_id_to_recipient_id[zulip_member_id]
            # PMs don't have topics, but topic_name field is required in `build_message`.
            message_dict["topic_name"] = ""
        elif message["rid"] in dsc_id_to_dsc_map:
            # Message is in a discussion
            dsc_channel = dsc_id_to_dsc_map[message["rid"]]
            parent_channel_id = dsc_channel["prid"]
            stream_id = stream_id_mapper.get(parent_channel_id)
            message_dict["recipient_id"] = stream_id_to_recipient_id[stream_id]
            message_dict["topic_name"] = "(Discussion) {}".format(dsc_channel["fname"])
        else:
            stream_id = stream_id_mapper.get(message["rid"])
            message_dict["recipient_id"] = stream_id_to_recipient_id[stream_id]
            message_dict["topic_name"] = "Imported from Rocket.Chat"

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

    raw_messages: List[Dict[str, Any]] = []
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


def separate_channel_and_private_messages(
    messages: List[Dict[str, Any]],
    direct_id_to_direct_map: Dict[str, Dict[str, Any]],
    channel_messages: List[Dict[str, Any]],
    private_messages: List[Dict[str, Any]],
) -> None:
    direct_channels_list = direct_id_to_direct_map.keys()
    for message in messages:
        if not message.get("rid"):
            # Message does not belong to any channel (might be
            # related to livechat), so ignore all such messages.
            continue
        if message["rid"] in direct_channels_list:
            private_messages.append(message)
        else:
            channel_messages.append(message)


def map_receiver_id_to_recipient_id(
    zerver_recipient: List[ZerverFieldsT],
    stream_id_to_recipient_id: Dict[int, int],
    user_id_to_recipient_id: Dict[int, int],
) -> None:
    # receiver_id represents stream_id/user_id
    for recipient in zerver_recipient:
        if recipient["type"] == Recipient.STREAM:
            stream_id_to_recipient_id[recipient["type_id"]] = recipient["id"]
        elif recipient["type"] == Recipient.PERSONAL:
            user_id_to_recipient_id[recipient["type_id"]] = recipient["id"]


def categorize_channels_and_map_with_id(
    channel_data: List[Dict[str, Any]],
    room_id_to_room_map: Dict[str, Dict[str, Any]],
    team_id_to_team_map: Dict[str, Dict[str, Any]],
    dsc_id_to_dsc_map: Dict[str, Dict[str, Any]],
    direct_id_to_direct_map: Dict[str, Dict[str, Any]],
) -> None:
    for channel in channel_data:
        if channel.get("prid"):
            dsc_id_to_dsc_map[channel["_id"]] = channel
        elif channel["t"] == "d":
            direct_id_to_direct_map[channel["_id"]] = channel
        else:
            room_id_to_room_map[channel["_id"]] = channel
            if channel.get("teamMain") is True:
                team_id_to_team_map[channel["teamId"]] = channel


def map_username_to_user_id(user_id_to_user_map: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    username_to_user_id_map: Dict[str, str] = {}
    for user_id, user_dict in user_id_to_user_map.items():
        username_to_user_id_map[user_dict["username"]] = user_id
    return username_to_user_id_map


def map_user_id_to_user(user_data_list: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    user_id_to_user_map = {}
    for user in user_data_list:
        user_id_to_user_map[user["_id"]] = user
    return user_id_to_user_map


def rocketchat_data_to_dict(rocketchat_data_dir: str) -> Dict[str, Any]:
    rocketchat_data: Dict[str, Any] = {}
    rocketchat_data["user"] = []
    rocketchat_data["avatar"] = {"avatar": [], "file": [], "chunk": []}
    rocketchat_data["room"] = []
    rocketchat_data["message"] = []

    # Get user
    with open(os.path.join(rocketchat_data_dir, "users.bson"), "rb") as fcache:
        rocketchat_data["user"] = bson.decode_all(fcache.read())

    # Get avatar
    with open(os.path.join(rocketchat_data_dir, "rocketchat_avatars.bson"), "rb") as fcache:
        rocketchat_data["avatar"]["avatar"] = bson.decode_all(fcache.read())

    with open(os.path.join(rocketchat_data_dir, "rocketchat_avatars.chunks.bson"), "rb") as fcache:
        rocketchat_data["avatar"]["chunk"] = bson.decode_all(fcache.read())

    with open(os.path.join(rocketchat_data_dir, "rocketchat_avatars.files.bson"), "rb") as fcache:
        rocketchat_data["avatar"]["file"] = bson.decode_all(fcache.read())

    # Get room
    with open(os.path.join(rocketchat_data_dir, "rocketchat_room.bson"), "rb") as fcache:
        rocketchat_data["room"] = bson.decode_all(fcache.read())

    # Get messages
    with open(os.path.join(rocketchat_data_dir, "rocketchat_message.bson"), "rb") as fcache:
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

    user_id_to_user_map: Dict[str, Dict[str, Any]] = map_user_id_to_user(rocketchat_data["user"])
    username_to_user_id_map: Dict[str, str] = map_username_to_user_id(user_id_to_user_map)

    user_handler = UserHandler()
    subscriber_handler = SubscriberHandler()
    user_id_mapper = IdMapper()
    stream_id_mapper = IdMapper()

    process_users(
        user_id_to_user_map=user_id_to_user_map,
        realm_id=realm_id,
        domain_name=domain_name,
        user_handler=user_handler,
        user_id_mapper=user_id_mapper,
    )

    room_id_to_room_map: Dict[str, Dict[str, Any]] = {}
    team_id_to_team_map: Dict[str, Dict[str, Any]] = {}
    dsc_id_to_dsc_map: Dict[str, Dict[str, Any]] = {}
    direct_id_to_direct_map: Dict[str, Dict[str, Any]] = {}

    categorize_channels_and_map_with_id(
        channel_data=rocketchat_data["room"],
        room_id_to_room_map=room_id_to_room_map,
        team_id_to_team_map=team_id_to_team_map,
        dsc_id_to_dsc_map=dsc_id_to_dsc_map,
        direct_id_to_direct_map=direct_id_to_direct_map,
    )

    zerver_stream = convert_channel_data(
        room_id_to_room_map=room_id_to_room_map,
        team_id_to_team_map=team_id_to_team_map,
        stream_id_mapper=stream_id_mapper,
        realm_id=realm_id,
    )
    realm["zerver_stream"] = zerver_stream

    # Add subscription data to subscriber handler
    convert_subscription_data(
        user_id_to_user_map=user_id_to_user_map,
        dsc_id_to_dsc_map=dsc_id_to_dsc_map,
        zerver_stream=zerver_stream,
        stream_id_mapper=stream_id_mapper,
        user_id_mapper=user_id_mapper,
        subscriber_handler=subscriber_handler,
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

    stream_id_to_recipient_id: Dict[int, int] = {}
    user_id_to_recipient_id: Dict[int, int] = {}

    map_receiver_id_to_recipient_id(
        zerver_recipient=zerver_recipient,
        stream_id_to_recipient_id=stream_id_to_recipient_id,
        user_id_to_recipient_id=user_id_to_recipient_id,
    )

    channel_messages: List[Dict[str, Any]] = []
    private_messages: List[Dict[str, Any]] = []

    separate_channel_and_private_messages(
        messages=rocketchat_data["message"],
        direct_id_to_direct_map=direct_id_to_direct_map,
        channel_messages=channel_messages,
        private_messages=private_messages,
    )

    total_reactions: List[ZerverFieldsT] = []

    # Process channel messages
    process_messages(
        realm_id=realm_id,
        messages=channel_messages,
        subscriber_map=subscriber_map,
        is_pm_data=False,
        username_to_user_id_map=username_to_user_id_map,
        user_id_mapper=user_id_mapper,
        user_handler=user_handler,
        user_id_to_recipient_id=user_id_to_recipient_id,
        stream_id_mapper=stream_id_mapper,
        stream_id_to_recipient_id=stream_id_to_recipient_id,
        direct_id_to_direct_map=direct_id_to_direct_map,
        dsc_id_to_dsc_map=dsc_id_to_dsc_map,
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
        username_to_user_id_map=username_to_user_id_map,
        user_id_mapper=user_id_mapper,
        user_handler=user_handler,
        user_id_to_recipient_id=user_id_to_recipient_id,
        stream_id_mapper=stream_id_mapper,
        stream_id_to_recipient_id=stream_id_to_recipient_id,
        direct_id_to_direct_map=direct_id_to_direct_map,
        dsc_id_to_dsc_map=dsc_id_to_dsc_map,
        # zerver_realmemoji=zerver_realmemoji,
        total_reactions=total_reactions,
        output_dir=output_dir,
    )
    realm["zerver_reaction"] = total_reactions
    realm["zerver_userprofile"] = user_handler.get_all_users()
    realm["sort_by_date"] = True

    create_converted_data_files(realm, output_dir, "/realm.json")
    # TODO: Add support for importing avatars
    create_converted_data_files([], output_dir, "/avatars/records.json")
    # TODO: Add support for importing uploads
    create_converted_data_files([], output_dir, "/uploads/records.json")

    # TODO: Add support for importing attachments
    attachment: Dict[str, List[Any]] = {"zerver_attachment": []}
    create_converted_data_files(attachment, output_dir, "/attachment.json")

    logging.info("Start making tarball")
    subprocess.check_call(["tar", "-czf", output_dir + ".tar.gz", output_dir, "-P"])
    logging.info("Done making tarball")
