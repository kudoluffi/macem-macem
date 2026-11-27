
import os
import re
from collections import defaultdict, Counter

import mysql.connector
from dotenv import load_dotenv

# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

INTERNAL_DOMAIN = "mydomain.com"
BATCH_SIZE = 10000
LOGICAL_MESSAGE_WINDOW_SECONDS = 120

# ============================================================
# REGEX
# ============================================================

MESSAGE_ID_PATTERN = re.compile(
    r"(?<!resent-)\bmessage-id=<([^>]+)>",
    re.IGNORECASE
)

QUEUED_AS_PATTERN = re.compile(
    r"\bqueued as ([A-Fa-f0-9]{8,16})\b",
    re.IGNORECASE
)

SIZE_PATTERN = re.compile(r"\bsize=(\d+)\b", re.IGNORECASE)

FROM_PATTERN = re.compile(
    r"from=<([^>]*)>", re.IGNORECASE
)

TO_PATTERN = re.compile(
    r"to=<([^>]*)>", re.IGNORECASE
)

CLIENT_PATTERN = re.compile(
    r"(?:client=|from\s+)(?:unknown|localhost|\S+)"
    r"\[([^\]]+)\]",
    re.IGNORECASE
)

SUBJECT_PATTERN = re.compile(
    r"warning:\s*header Subject:\s*(.*?)"
    r"\s+from\s+(?:unknown|localhost|\S+)"
    r"(?:\[[^\]]*\])?(?=;|$)",
    re.IGNORECASE
)

STATUS_PATTERN = re.compile(
    r"\bstatus=(sent|deferred|bounced|expired)\b",
    re.IGNORECASE
)

DSN_PATTERN = re.compile(
    r"\bdsn=([0-9]\.[0-9]\.[0-9]+)",
    re.IGNORECASE
)

RELAY_PATTERN = re.compile(
    r"\brelay=([^,\s]+)", re.IGNORECASE
)

FILENAME_PATTERN = re.compile(
    r'(?:filename|name)\s*=\s*'
    r'(?:"([^"]+)"|([^;\s]+))',
    re.IGNORECASE
)

CONTENT_TYPE_PATTERN = re.compile(
    r"Content-Type:\s*([^;\s]+)",
    re.IGNORECASE
)

DISPOSITION_PATTERN = re.compile(
    r"Content-Disposition:\s*"
    r"(attachment|inline|form-data)",
    re.IGNORECASE
)

# ============================================================
# DATABASE
# ============================================================

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connection_timeout=20
    )


# ============================================================
# HELPERS
# ============================================================

def normalize_email(value):
    if not value:
        return None

    value = value.strip().lower()

    if "@" not in value:
        return None

    return value


def is_internal(email):
    email = normalize_email(email)

    if not email:
        return False

    return email.rsplit("@", 1)[1] == INTERNAL_DOMAIN


def classify_direction(sender, recipients):
    sender_internal = is_internal(sender)

    recipient_domains = [
        is_internal(r)
        for r in recipients
        if normalize_email(r)
    ]

    if not recipient_domains:
        return "unknown"

    if sender_internal:
        if all(recipient_domains):
            return "internal"
        return "outbound"

    if any(recipient_domains):
        return "inbound"

    return "unknown"


def extract_message_id(raw):
    match = MESSAGE_ID_PATTERN.search(raw or "")
    return match.group(1).strip() if match else None


def extract_queued_as(raw):
    match = QUEUED_AS_PATTERN.search(raw or "")
    return match.group(1).upper() if match else None


def extract_sender(raw):
    match = FROM_PATTERN.search(raw or "")
    return normalize_email(match.group(1)) if match else None


def extract_recipient(raw):
    match = TO_PATTERN.search(raw or "")
    return normalize_email(match.group(1)) if match else None


def extract_subject(raw):
    match = SUBJECT_PATTERN.search(raw or "")

    if not match:
        return None

    return match.group(1).strip()


def extract_client_ip(raw):
    match = CLIENT_PATTERN.search(raw or "")
    return match.group(1).strip() if match else None


def extract_size(raw):
    match = SIZE_PATTERN.search(raw or "")
    return int(match.group(1)) if match else None


def extract_delivery(raw):
    raw = raw or ""

    status_match = STATUS_PATTERN.search(raw)
    relay_match = RELAY_PATTERN.search(raw)
    dsn_match = DSN_PATTERN.search(raw)

    if not status_match:
        return None

    status = status_match.group(1).lower()
    relay = relay_match.group(1) if relay_match else None
    dsn = dsn_match.group(1) if dsn_match else None

    response_match = re.search(
        r"status=[^ ]+\s+\((.*)\)\s*$",
        raw
    )

    response = (
        response_match.group(1).strip()
        if response_match else None
    )

    return {
        "status": status,
        "relay": relay,
        "dsn": dsn,
        "response": response
    }


def is_final_delivery(process_name, delivery):
    process = (process_name or "").lower()
    relay = (delivery.get("relay") or "").lower()

    if not relay:
        return False

    # Semua relay localhost adalah tahap perantara,
    # kecuali LMTP lokal pada port 7025.

    if "postfix/lmtp" in process:
        return ":7025" in relay

    if "postfix/smtp" not in process:
        return False

    if relay.startswith("127.0.0.1"):
        return False

    if relay.startswith("localhost"):
        return False

    return True


def choose_most_common(values):
    values = [v for v in values if v]

    if not values:
        return None

    return Counter(values).most_common(1)[0][0]


def normalize_subject(value):
    if not value:
        return ""

    return " ".join(value.strip().lower().split())


# ============================================================
# READ EVENTS
# ============================================================

def read_events(cursor):
    last_id = 0
    total = 0

    while True:
        cursor.execute(
            """
            SELECT
                id,
                queue_id,
                event_time,
                process_name,
                event_type,
                sender,
                recipient,
                raw_log
            FROM mail_queue_events
            WHERE id > %s
            ORDER BY id
            LIMIT %s
            """,
            (last_id, BATCH_SIZE)
        )

        rows = cursor.fetchall()

        if not rows:
            break

        for row in rows:
            yield row

        total += len(rows)
        last_id = rows[-1]["id"]

        print(
            f"  Dibaca: {total:,} event",
            flush=True
        )


# ============================================================
# QUEUE DATA
# ============================================================

def new_queue():
    return {
        "first_seen": None,
        "last_seen": None,
        "message_ids": set(),
        "senders": set(),
        "recipients": set(),
        "subjects": set(),
        "client_ips": set(),
        "sizes": set(),
        "attachments": set(),
        "deliveries": {},
        "event_count": 0
    }


def update_time(queue, event_time):
    if not event_time:
        return

    if queue["first_seen"] is None or event_time < queue["first_seen"]:
        queue["first_seen"] = event_time

    if queue["last_seen"] is None or event_time > queue["last_seen"]:
        queue["last_seen"] = event_time


# ============================================================
# QUEUE LINEAGE
# ============================================================

def build_lineage(queue_ids, parent_candidates):
    queue_ids = set(queue_ids)

    parent_map = {}
    conflicting_parents = 0
    missing_parent_edges = set()

    # Hanya gunakan relasi yang child-nya merupakan Queue ID internal.
    for child, parents in parent_candidates.items():

        if child not in queue_ids:
            continue

        valid_parents = set()

        for parent in parents:
            if parent in queue_ids:
                valid_parents.add(parent)
            else:
                missing_parent_edges.add((child, parent))

        # Jika terdapat lebih dari satu parent internal,
        # jangan memilih salah satunya secara otomatis.
        if len(valid_parents) > 1:
            conflicting_parents += 1
            continue

        if len(valid_parents) == 1:
            parent_map[child] = next(iter(valid_parents))

    root_map = {}
    cycles = set()

    for queue_id in queue_ids:

        current = queue_id
        visited = set()

        while current in parent_map:

            if current in visited:
                cycles.add(queue_id)
                break

            visited.add(current)
            current = parent_map[current]

        root_map[queue_id] = current

    return (
        root_map,
        parent_map,
        conflicting_parents,
        len(missing_parent_edges),
        cycles
    )

# ============================================================
# TRANSACTION ANALYSIS
# ============================================================

def build_transactions(queues, root_map):
    transactions = defaultdict(list)

    for queue_id, root_id in root_map.items():
        transactions[root_id].append(queue_id)

    result = {}

    for root_id, members in transactions.items():

        message_ids = set()
        senders = set()
        recipients = set()
        subjects = set()
        client_ips = set()
        sizes = set()
        deliveries = {}
        attachments = set()

        first_seen = None
        last_seen = None

        for queue_id in members:

            q = queues[queue_id]

            message_ids.update(q["message_ids"])
            senders.update(q["senders"])
            recipients.update(q["recipients"])
            subjects.update(q["subjects"])
            client_ips.update(q["client_ips"])
            sizes.update(q["sizes"])
            attachments.update(q["attachments"])

            if q["first_seen"]:
                if first_seen is None or q["first_seen"] < first_seen:
                    first_seen = q["first_seen"]

            if q["last_seen"]:
                if last_seen is None or q["last_seen"] > last_seen:
                    last_seen = q["last_seen"]

            for recipient, delivery in q["deliveries"].items():

                previous = deliveries.get(recipient)

                if (
                    previous is None
                    or delivery["event_time"] >= previous["event_time"]
                ):
                    deliveries[recipient] = delivery

        sender = choose_most_common(senders)
        subject = choose_most_common(subjects)
        client_ip = choose_most_common(client_ips)
        message_size = max(sizes) if sizes else None

        result[root_id] = {
            "queue_ids": sorted(members),
            "message_ids": message_ids,
            "sender": sender,
            "senders": senders,
            "subject": subject,
            "subjects": subjects,
            "client_ip": client_ip,
            "message_size": message_size,
            "recipients": recipients,
            "deliveries": deliveries,
            "attachments": attachments,
            "first_seen": first_seen,
            "last_seen": last_seen,
            "direction": classify_direction(sender, recipients)
        }

    return result


# ============================================================
# LOGICAL MESSAGE CANDIDATES
# ============================================================

def build_logical_candidates(transactions):
    """
    MODE KONSERVATIF:
    Satu transaksi SMTP dianggap sebagai satu Logical Message.

    Message-ID hanya menjadi atribut pencarian,
    bukan dasar untuk menggabungkan transaksi.
    """

    groups = []

    for root_id in transactions:
        groups.append([root_id])

    return groups

# ============================================================
# MAIN DRY RUN
# ============================================================

def main():

    print("=" * 70)
    print("ZIMBRA MAIL TRACKER - HISTORICAL NORMALIZER")
    print("VERSION 1 - DRY RUN / READ ONLY")
    print("=" * 70)

    db = connect_db()
    cursor = db.cursor(dictionary=True)

    try:

        queues = defaultdict(new_queue)
        queue_ids = set()

        parent_candidates = defaultdict(set)
        queue_links = set()

        event_types = Counter()

        total_events = 0
        message_id_events = 0
        queued_as_events = 0
        attachment_events = 0
        final_delivery_events = 0
        ignored_intermediate_deliveries = 0

        print("\n[1] Membaca event database...")

        for row in read_events(cursor):

            total_events += 1

            queue_id = row["queue_id"]

            if not queue_id:
                continue

            queue_id = queue_id.upper()
            queue_ids.add(queue_id)

            q = queues[queue_id]

            q["event_count"] += 1

            event_type = row["event_type"] or "NULL"
            event_types[event_type] += 1

            raw = row["raw_log"] or ""
            process = row["process_name"] or ""

            update_time(q, row["event_time"])

            # Message-ID

            message_id = extract_message_id(raw)

            if message_id:
                q["message_ids"].add(message_id)
                message_id_events += 1

            # Queue lineage

            child_queue = extract_queued_as(raw)

            if child_queue:
                child_queue = child_queue.upper()

                if child_queue != queue_id:
                    queue_links.add((queue_id, child_queue))
                    parent_candidates[child_queue].add(queue_id)

                queued_as_events += 1

            # Sender

            sender = normalize_email(row["sender"])

            if not sender:
                sender = extract_sender(raw)

            if sender:
                q["senders"].add(sender)

            # Recipient

            recipient = normalize_email(row["recipient"])

            if not recipient:
                recipient = extract_recipient(raw)

            if recipient:
                q["recipients"].add(recipient)

            # Subject

            subject = row.get("subject") if "subject" in row else None

            if not subject:
                subject = extract_subject(raw)

            if subject:
                q["subjects"].add(subject.strip())

            # Client IP

            client_ip = extract_client_ip(raw)

            if client_ip:
                q["client_ips"].add(client_ip)

            # Message size

            size = extract_size(raw)

            if size is not None:
                q["sizes"].add(size)

            # Attachment

            if event_type == "attachment_type":

                filename_match = FILENAME_PATTERN.search(raw)
                mime_match = CONTENT_TYPE_PATTERN.search(raw)
                disposition_match = DISPOSITION_PATTERN.search(raw)

                filename = None

                if filename_match:
                    filename = (
                        filename_match.group(1)
                        or filename_match.group(2)
                    )

                mime_type = (
                    mime_match.group(1).strip()
                    if mime_match else None
                )

                disposition = (
                    disposition_match.group(1).lower()
                    if disposition_match else "unknown"
                )

                if filename:
                    q["attachments"].add(
                        (
                            filename.strip(),
                            mime_type,
                            disposition
                        )
                    )

                    attachment_events += 1

            # Delivery

            if event_type == "delivery":

                delivery = extract_delivery(raw)

                if delivery:

                    if is_final_delivery(process, delivery):

                        final_delivery_events += 1

                        if recipient:

                            delivery["event_time"] = row["event_time"]

                            previous = q["deliveries"].get(recipient)

                            if (
                                previous is None
                                or delivery["event_time"] >= previous["event_time"]
                            ):
                                q["deliveries"][recipient] = delivery

                    else:
                        ignored_intermediate_deliveries += 1

        print("\n[2] Membangun hubungan Queue ID...")

        (
            root_map,
            parent_map,
            conflicting_parents,
            missing_parent_edges,
            cycles
        ) = build_lineage(queue_ids, parent_candidates)

        transactions = build_transactions(queues, root_map)

        print("\n[3] Menganalisis kandidat Logical Message...")

        logical_groups = build_logical_candidates(transactions)

        grouped_transactions = sum(
            len(group)
            for group in logical_groups
            if len(group) > 1
        )

        multi_queue_transactions = sum(
            1 for tx in transactions.values()
            if len(tx["queue_ids"]) > 1
        )

        multi_message_queues = sum(
            1 for q in queues.values()
            if len(q["message_ids"]) > 1
        )

        sender_conflicts = sum(
            1 for tx in transactions.values()
            if len(tx["senders"]) > 1
        )

        subject_conflicts = sum(
            1 for tx in transactions.values()
            if len(tx["subjects"]) > 1
        )

        final_recipient_statuses = Counter()

        for tx in transactions.values():
            for delivery in tx["deliveries"].values():
                final_recipient_statuses[delivery["status"]] += 1

        attachment_count = sum(
            len(tx["attachments"])
            for tx in transactions.values()
        )

        direction_counts = Counter(
            tx["direction"]
            for tx in transactions.values()
        )

        print("\n" + "=" * 70)
        print("HASIL DRY RUN")
        print("=" * 70)

        print(f"Total raw events                 : {total_events:,}")
        print(f"Total Queue ID                   : {len(queue_ids):,}")
        print(f"Relasi queued as                 : {len(queue_links):,}")
        print(f"Queue ID yang menjadi turunan    : {len(parent_map):,}")
        print(f"Estimasi transaksi SMTP awal     : {len(transactions):,}")
        print(f"Transaksi dengan beberapa Queue  : {multi_queue_transactions:,}")

        print("\n--- MESSAGE ---")
        print(f"Event dengan Message-ID          : {message_id_events:,}")
        print(f"Kandidat Logical Message         : {len(logical_groups):,}")
        print(f"Transaksi dalam grup kandidat    : {grouped_transactions:,}")
        print(f"Queue dengan beberapa Message-ID : {multi_message_queues:,}")

        print("\n--- TRANSACTION QUALITY ---")
        print(f"Konflik sender per transaksi     : {sender_conflicts:,}")
        print(f"Konflik subject per transaksi    : {subject_conflicts:,}")
        print(f"Relasi dengan parent tidak ada   : {missing_parent_edges:,}")
        print(f"Queue dengan parent konflik      : {conflicting_parents:,}")
        print(f"Queue yang terindikasi siklus    : {len(cycles):,}")

        print("\n--- DELIVERY ---")
        print(f"Event delivery final             : {final_delivery_events:,}")
        print(f"Event perantara diabaikan        : {ignored_intermediate_deliveries:,}")

        for status, count in final_recipient_statuses.most_common():
            print(f"  {status:<28}: {count:,}")

        print("\n--- ATTACHMENT ---")
        print(f"Event attachment_type            : {attachment_events:,}")
        print(f"Kandidat attachment unik         : {attachment_count:,}")

        print("\n--- DIRECTION ---")

        for direction, count in direction_counts.most_common():
            print(f"  {direction:<28}: {count:,}")

        print("\n--- EVENT TYPES ---")

        for name, count in event_types.most_common():
            print(f"  {name:<28}: {count:,}")

        print("\n" + "=" * 70)
        print("PEMERIKSAAN SELESAI")
        print("READ ONLY - TIDAK ADA DATA DATABASE YANG DIUBAH")
        print("=" * 70)

    finally:
        cursor.close()
        db.close()


if __name__ == "__main__":
    main()
