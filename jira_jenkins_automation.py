#!/usr/bin/env python3
"""
Jira-Jenkins test execution automation for Hardware-in-the-Loop (HIL) testing.

Polls Jira for new "Test Execution" tickets, groups them by software prefix,
reserves HIL test nodes (thread-safe), triggers Jenkins flashing and execution
jobs, and writes the results back to Jira.

All credentials, URLs and environment-specific values are read from
environment variables (see .env.example) and sw_profiles.json.
"""
import base64
import json
import logging
import os
import random
import re
import smtplib
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# -------------------- Configuration --------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def env(name, default=""):
    return os.getenv(name, default)


# Email notifications (optional)
SMTP_SERVER = env('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(env('SMTP_PORT', '587'))
EMAIL_USER = env('EMAIL_USER')
EMAIL_PASS = env('EMAIL_PASS')
NOTIFICATION_EMAIL = env('NOTIFICATION_EMAIL')

# Jira
jira_url = env('JIRA_URL').rstrip('/')
jira_headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {env('JIRA_TOKEN')}",
    "Content-Type": "application/json",
    "X-Atlassian-Token": "no-check"
}

# Jira custom field IDs (override in your environment)
FIELD_TESTCASES = env('JIRA_FIELD_TESTCASES', 'customfield_10001')
FIELD_BRANCH = env('JIRA_FIELD_BRANCH', 'customfield_10002')
FIELD_SOP = env('JIRA_FIELD_SOP', 'customfield_10003')
FIELD_SW_VERSION = env('JIRA_FIELD_SW_VERSION', 'customfield_10004')
FIELD_TEST_BENCHES = env('JIRA_FIELD_TEST_BENCHES', 'customfield_10005')
FIELD_DOMAINS = env('JIRA_FIELD_DOMAINS', 'customfield_10006')
FIELD_HW_VERSION = env('JIRA_FIELD_HW_VERSION', 'customfield_10007')
FIELD_GROUP_NUMBER = env('JIRA_FIELD_GROUP_NUMBER', 'customfield_10008')

JIRA_FIELDS = ",".join([
    FIELD_TESTCASES, FIELD_BRANCH, FIELD_SOP, FIELD_SW_VERSION,
    FIELD_TEST_BENCHES, FIELD_DOMAINS, FIELD_HW_VERSION, FIELD_GROUP_NUMBER,
    "description", "status"
])

# Jira workflow transition IDs (override in your environment)
TRANSITION_IN_PROGRESS = env('JIRA_TRANSITION_IN_PROGRESS', '11')
TRANSITION_DONE = env('JIRA_TRANSITION_DONE', '21')
TRANSITION_WAITING_FEEDBACK = env('JIRA_TRANSITION_WAITING_FEEDBACK', '31')

# Jenkins
jenkins_base_url = env('JENKINS_URL').rstrip('/')
jenkins_flashing_url = f"{jenkins_base_url}/generic-webhook-trigger/invoke"
jenkins_flashing_token = env('JENKINS_FLASHING_TOKEN')
execution_job_token = env('JENKINS_EXECUTION_TOKEN')
jenkins_username = env('JENKINS_USERNAME')
jenkins_api_token = env('JENKINS_API_TOKEN')
auth_b64 = base64.b64encode(f"{jenkins_username}:{jenkins_api_token}".encode()).decode()
jenkins_headers = {"Content-Type": "application/json"}
jenkins_headers_for_api = {
    "Authorization": f"Basic {auth_b64}",
    "Content-Type": "application/json"
}
FLASHING_JOB_NAME = env('FLASHING_JOB_NAME', 'flashing-job')
EXECUTION_JOB_NAME = env('EXECUTION_JOB_NAME', 'execution-job')

# Node scheduler / DUT request
dut_request_url = env('DUT_REQUEST_URL')
bts_scheduler_url = env('BTS_SCHEDULER_URL')
DUT_CUSTOMER = env('DUT_CUSTOMER', 'CUSTOMER')
DUT_PROJECT = env('DUT_PROJECT', 'PROJECT')
DUT_KEY_TYPE = env('DUT_KEY_TYPE', 'KEY')
DUT_LOCATION = env('DUT_LOCATION', 'LOCATION')

# Job / test settings
NODE_ACTIVITY = env('NODE_ACTIVITY', 'Execution')
DEFAULT_DOMAIN = env('DEFAULT_DOMAIN', 'DEFAULT')
DEFAULT_SOP = env('DEFAULT_SOP', '00-00__DEFAULT')
KEYWORD_VERSION = env('KEYWORD_VERSION', '1.0.0')
HW_TYPE = env('HW_TYPE', 'HW')
SW_ARTIFACT_PATH_TEMPLATE = env('SW_ARTIFACT_PATH_TEMPLATE', 'releases/{sw}/package.zip')

# Software profiles: maps a SW prefix (e.g. "20.") to HW version and job settings
SW_PROFILES_FILE = env('SW_PROFILES_FILE', 'sw_profiles.json')


def load_sw_profiles():
    try:
        with open(SW_PROFILES_FILE, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"SW profiles file '{SW_PROFILES_FILE}' not found, using empty profiles")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in '{SW_PROFILES_FILE}': {e}")
        return {}


SW_PROFILES = load_sw_profiles()


def get_sw_profile(sw):
    """Return the profile whose key is a prefix of the given SW version."""
    if sw:
        for key, profile in SW_PROFILES.items():
            if sw.startswith(key):
                return profile
    return {}


def validate_config():
    """Exit with a clear message if required settings are missing."""
    required = {
        'JIRA_URL': jira_url,
        'JIRA_TOKEN': env('JIRA_TOKEN'),
        'JENKINS_URL': jenkins_base_url,
        'JENKINS_USERNAME': jenkins_username,
        'JENKINS_API_TOKEN': jenkins_api_token,
        'JENKINS_FLASHING_TOKEN': jenkins_flashing_token,
        'JENKINS_EXECUTION_TOKEN': execution_job_token,
        'DUT_REQUEST_URL': dut_request_url,
        'BTS_SCHEDULER_URL': bts_scheduler_url,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")


# Global tracking - individual issues and failures
group_locks = {}  # Threading locks for each group
processed_issue_keys = set()  # Individual issues that are completed successfully
failed_issue_keys = set()  # Individual issues that have failed (flashing or execution)
waiting_feedback_issue_keys = set()  # Issues currently in "Waiting for feedback" status
failed_sw_groups = set()  # SW groups that have failed flashing (format: "prefix_sw_version")
group_processing_status = {}  # Track current status of each group

# -------------------- NODE-LEVEL COORDINATION --------------------

# Global node-level coordination to prevent simultaneous flashing
node_locks = {}  # node_name -> threading.Lock()
node_current_state = {}  # node_name -> {sw_version, status, last_updated, group_id, prefix, thread_name}
node_state_lock = threading.Lock()  # Protects node_locks and node_current_state dictionaries


def get_or_create_node_lock(node_name):
    """Get or create a lock for a specific node (thread-safe)."""
    with node_state_lock:
        if node_name not in node_locks:
            node_locks[node_name] = threading.Lock()
            logger.debug(f"Created lock for node {node_name}")
        return node_locks[node_name]


def reserve_node_for_sw(node_name, target_sw, group_id, prefix, operation="FLASHING"):
    """
    Reserve a node for specific SW processing.
    Returns True if successfully reserved, False if already busy.
    This is the key function that prevents simultaneous flashing.
    """
    node_lock = get_or_create_node_lock(node_name)
    thread_name = threading.current_thread().name

    with node_lock:
        current_state = node_current_state.get(node_name)

        if current_state:
            if current_state['status'] == 'BUSY':
                current_sw = current_state.get('sw_version', 'UNKNOWN')
                current_group = current_state.get('group_id', 'UNKNOWN')
                current_prefix = current_state.get('prefix', 'UNKNOWN')
                current_thread = current_state.get('thread_name', 'UNKNOWN')

                logger.warning(f"[{thread_name}] Node {node_name} is BUSY with SW {current_sw} (Group {current_group}, Prefix {current_prefix}, Thread {current_thread}). Cannot reserve for SW {target_sw} (Group {group_id}, Prefix {prefix})")
                return False

        node_current_state[node_name] = {
            'sw_version': target_sw,
            'status': 'BUSY',
            'group_id': group_id,
            'prefix': prefix,
            'operation': operation,
            'thread_name': thread_name,
            'last_updated': time.time()
        }

        logger.info(f"[{thread_name}] Successfully reserved node {node_name} for SW {target_sw} (Group {group_id}, Prefix {prefix}, Operation: {operation})")
        return True


def release_node(node_name):
    """Release a node from reservation."""
    node_lock = get_or_create_node_lock(node_name)
    thread_name = threading.current_thread().name

    with node_lock:
        if node_name in node_current_state:
            released_state = node_current_state.pop(node_name)
            logger.info(f"[{thread_name}] Released node {node_name} (was processing SW {released_state.get('sw_version', 'UNKNOWN')} for Group {released_state.get('group_id', 'UNKNOWN')} by Thread {released_state.get('thread_name', 'UNKNOWN')})")
        else:
            logger.debug(f"[{thread_name}] Node {node_name} was not reserved")


def get_node_status(node_name):
    """Get current status of a node (thread-safe)."""
    node_lock = get_or_create_node_lock(node_name)

    with node_lock:
        return node_current_state.get(node_name, {'status': 'FREE'})


def get_all_node_statuses():
    """Get status of all nodes for debugging."""
    with node_state_lock:
        statuses = {}
        for node_name, state in node_current_state.items():
            statuses[node_name] = {
                'status': state.get('status', 'UNKNOWN'),
                'sw_version': state.get('sw_version', 'UNKNOWN'),
                'group_id': state.get('group_id', 'UNKNOWN'),
                'prefix': state.get('prefix', 'UNKNOWN'),
                'thread_name': state.get('thread_name', 'UNKNOWN'),
                'operation': state.get('operation', 'UNKNOWN')
            }
        return statuses


def cleanup_stale_reservations(max_age_hours=24):
    """Clean up stale node reservations (nodes reserved for too long)."""
    current_time = time.time()
    max_age_seconds = max_age_hours * 3600

    with node_state_lock:
        stale_nodes = []
        for node_name, state in node_current_state.items():
            if current_time - state.get('last_updated', 0) > max_age_seconds:
                stale_nodes.append(node_name)

        for node_name in stale_nodes:
            released_state = node_current_state.pop(node_name)
            logger.warning(f"Released stale reservation for node {node_name} (SW {released_state.get('sw_version', 'UNKNOWN')}, Group {released_state.get('group_id', 'UNKNOWN')}, Thread {released_state.get('thread_name', 'UNKNOWN')})")


# -------------------- Helper Functions --------------------

def get_bts_nodes():
    """Fetch test node information from the scheduler."""
    try:
        resp = requests.get(bts_scheduler_url, timeout=30)
        resp.raise_for_status()
        nodes = resp.json()
        logger.info(f"Retrieved {len(nodes)} nodes from scheduler")
        return nodes
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get nodes: {e}")
        return []


def normalize_sw_version(sw_version):
    """
    Normalize SW version to bare format (without leading zeros).
    Handles node format 'R020.022.040' -> '20.22.40'
    And issue format '0xx.0xx.0xx' or 'xx.xx.xx' -> 'xx.xx.xx'
    """
    if not sw_version:
        return None

    # Handle node format (starts with 'R')
    if sw_version.startswith('R'):
        sw_version = sw_version[1:]

    parts = sw_version.split('.')
    try:
        normalized_parts = [str(int(part)) for part in parts]
        normalized = ".".join(normalized_parts)
        logger.debug(f"Normalized SW version: '{sw_version}' -> '{normalized}'")
        return normalized
    except ValueError:
        logger.debug(f"Could not normalize SW version: '{sw_version}'")
        return sw_version


def sort_sw_versions_ascending(sw_versions):
    """
    Sort SW versions in ascending order based on the logic:
    1. Compare middle number (.xx.)
    2. If middle numbers are equal, compare right number (.xx)
    3. If both middle and right are equal, compare left number (xx.)

    Example: 101.20.13 < 101.20.20 < 101.30.10 < 101.30.20 < 101.40.20
    """
    def sw_sort_key(sw):
        try:
            parts = sw.split('.')
            if len(parts) >= 3:
                left = int(parts[0])
                middle = int(parts[1])
                right = int(parts[2])
                return (middle, right, left)
            else:
                return (0, 0, int(parts[0]) if parts else 0)
        except (ValueError, IndexError):
            return (999999, 999999, 999999)

    sorted_versions = sorted(sw_versions, key=sw_sort_key)
    logger.info(f"Sorted SW versions: {sorted_versions}")
    return sorted_versions


def find_node_with_3_tier_selection_coordinated(sw_version, bts_nodes, group_id, prefix):
    """
    Apply 3-tier node selection WITH node-level coordination:
    1. Look for exact SW + execution-activity match that's available
    2. If not found, look for any available execution-activity node
    3. Otherwise no node is reserved

    Returns tuple: (node_name, selection_type, skip_flashing, reserved)
    selection_type: "exact_match", "qc_execution_only", "fallback"
    skip_flashing: True for exact_match, False otherwise
    reserved: True if node was successfully reserved, False otherwise
    """
    normalized_target_sw = normalize_sw_version(sw_version)
    thread_name = threading.current_thread().name

    logger.info(f"[{thread_name}] Applying coordinated 3-tier selection for SW {normalized_target_sw} (Group {group_id})")

    exact_match_nodes = []
    qc_execution_nodes = []

    for node in bts_nodes:
        node_sw_raw = normalize_sw_version(node.get('sw', ''))
        node_sw = normalize_sw_version(node_sw_raw)
        node_activity = node.get('activity', '')
        node_name = node['bts_name']

        if node_activity == NODE_ACTIVITY:
            node_status = get_node_status(node_name)
            if node_status['status'] == 'FREE':
                qc_execution_nodes.append(node_name)

                if node_sw == normalized_target_sw:
                    exact_match_nodes.append(node_name)
                    logger.debug(f"[{thread_name}] Found available exact match node: {node_name} with SW {node_sw}")

    # Tier 1: Try to reserve exact match if found
    for node_name in exact_match_nodes:
        if reserve_node_for_sw(node_name, sw_version, group_id, prefix):
            logger.info(f"[{thread_name}] Tier 1 - Reserved exact SW + execution match: {node_name}")
            return node_name, "exact_match", True, True

    # Tier 2: Try to reserve any available execution node
    for node_name in qc_execution_nodes:
        if reserve_node_for_sw(node_name, sw_version, group_id, prefix):
            logger.info(f"[{thread_name}] Tier 2 - Reserved available execution node: {node_name}")
            return node_name, "qc_execution_only", False, True

    # Tier 3: nothing available
    logger.warning(f"[{thread_name}] No available node could be reserved for SW {normalized_target_sw}")
    return None, "fallback", False, False


def extract_testcases(testcases_field):
    """Extract and format testcases from custom field. Handles various formats."""
    if not testcases_field:
        logger.debug("No testcases found")
        return []

    testcases = re.split(r'[,;\n\r\s]+', testcases_field.strip())
    testcases = [tc.strip() for tc in testcases if tc.strip()]
    logger.debug(f"Extracted testcases: {testcases}")
    return testcases


def extract_domains_from_description(description):
    """
    Extract domains from a description field that contains lines like:
    'testcases : TC_001,TC_002\nDomain : Climate\nSOP : 00-00__X\n...'
    """
    if not description:
        logger.debug("No description found, using default domain")
        return DEFAULT_DOMAIN

    domain_pattern = r'Domain\s*:\s*([^\n\r]+)'
    match = re.search(domain_pattern, description, re.IGNORECASE)

    if match:
        domain_value = match.group(1).strip()
        logger.debug(f"Extracted domain from description: '{domain_value}'")
        return domain_value

    logger.debug("No domain found in description, using default")
    return DEFAULT_DOMAIN


def extract_domains(domains_field):
    """
    Extract and format domains from custom field.
    Returns comma-separated string for multiple choices or single choice.
    """
    if not domains_field:
        logger.debug("No domains found, using default")
        return DEFAULT_DOMAIN

    if isinstance(domains_field, list):
        domains = []
        for domain_obj in domains_field:
            if isinstance(domain_obj, dict) and 'value' in domain_obj:
                domains.append(domain_obj['value'].strip())
            else:
                domains.append(str(domain_obj).strip())
        result = ",".join(domains) if domains else DEFAULT_DOMAIN
    elif isinstance(domains_field, dict) and 'value' in domains_field:
        result = domains_field['value'].strip()
    else:
        result = str(domains_field).strip()

    logger.debug(f"Extracted domains: '{result}' from '{domains_field}'")
    return result if result else DEFAULT_DOMAIN


def extract_sop(sop_field):
    """
    Extract SOP value from custom field.
    Handles both object format and string format.
    """
    if not sop_field:
        return DEFAULT_SOP

    if isinstance(sop_field, dict) and 'value' in sop_field:
        # Object format: {"value": "00-00_X", "id": "10000"}
        sop_value = sop_field['value']
        # Replace underscore with double underscore for job compatibility
        return sop_value.replace('_', '__')
    else:
        sop_str = str(sop_field).strip()
        return sop_str.replace('_', '__') if sop_str else DEFAULT_SOP


def extract_type(type_field):
    """
    Extract TYPE value from Test benches field.
    Handles both object format and string format.
    """
    if not type_field:
        return "ANY"

    if isinstance(type_field, list) and len(type_field) > 0:
        # Array format: [{"value": "TYPE_A", "id": "10000"}]
        first_item = type_field[0]
        if isinstance(first_item, dict) and 'value' in first_item:
            return first_item['value']
        else:
            return str(first_item)
    elif isinstance(type_field, dict) and 'value' in type_field:
        return type_field['value']
    else:
        return str(type_field).strip() if str(type_field).strip() else "ANY"


def get_jira_issues():
    """Retrieve all Test Execution issues in the active sprint with required custom fields."""
    jql = 'issuetype="Test Execution" AND Sprint in openSprints() AND status IN ("To Do")'

    url = f"{jira_url}/rest/api/2/search?jql={requests.utils.quote(jql)}&fields={JIRA_FIELDS}&maxResults=500"

    try:
        resp = requests.get(url, headers=jira_headers, timeout=30)
        resp.raise_for_status()
        issues = resp.json().get("issues", [])
        logger.info(f"Retrieved {len(issues)} Test Execution issues from Jira")

        if issues:
            sample_issue = issues[0]
            logger.debug(f"Sample issue fields for {sample_issue['key']}: {sample_issue['fields']}")

        return issues
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get Jira issues: {e}")
        return []


def check_if_issue_in_waiting_feedback():
    """Check and update the set of issues currently in 'Waiting for feedback' status."""
    global waiting_feedback_issue_keys

    jql = 'issuetype="Test Execution" AND Sprint in openSprints() AND status = "Waiting for feedback"'
    fields = "status"

    url = f"{jira_url}/rest/api/2/search?jql={requests.utils.quote(jql)}&fields={fields}&maxResults=500"

    try:
        resp = requests.get(url, headers=jira_headers, timeout=30)
        resp.raise_for_status()
        issues = resp.json().get("issues", [])

        current_waiting_issues = {issue['key'] for issue in issues}
        waiting_feedback_issue_keys.update(current_waiting_issues)

        logger.info(f"Found {len(current_waiting_issues)} issues currently in 'Waiting for feedback' status")

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to check issues in waiting feedback status: {e}")


def extract_sw_bare_format(sw_version_field):
    """
    Extract SW version in bare format (without padded zeros) from custom field.
    Handles formats like: xx.xx.xx, 0xx.0xx.0xx, xxx.0xx.0xx, xxx.xx.xx
    Returns the bare format: xx.xx.xx
    """
    if not sw_version_field:
        return None

    if re.match(r'^\d+\.\d+\.\d+$', sw_version_field):
        parts = sw_version_field.split('.')
        try:
            bare_parts = [str(int(part)) for part in parts]
            bare_sw = ".".join(bare_parts)
            logger.debug(f"Extracted SW (bare format): '{bare_sw}' from '{sw_version_field}'")
            return bare_sw
        except ValueError:
            logger.debug(f"Could not convert SW version: '{sw_version_field}'")
            return sw_version_field

    logger.debug(f"SW version format not recognized: '{sw_version_field}'")
    return sw_version_field


def get_hw_version_from_sw(sw):
    """Determine HW version from the SW prefix using the SW profiles."""
    if not sw:
        return "Unknown"
    return get_sw_profile(sw).get('hw_version', "Unknown")


def extract_sw_prefix(sw):
    """Extract SW prefix (first number + dot)."""
    if sw:
        parts = sw.split(".")
        prefix = parts[0] + "." if len(parts) > 0 else None
        logger.debug(f"SW prefix for {sw}: {prefix}")
        return prefix
    return None


def zero_pad_sw(sw):
    """Convert SW like xx.xx.xx to 0xx.0xx.0xx for DUT request/execution."""
    if not sw:
        return sw
    parts = sw.split(".")
    padded = ".".join([p.zfill(3) for p in parts])
    logger.debug(f"Zero-padded SW {sw} -> {padded}")
    return padded


def update_issue_custom_fields(issue_key, hw_version=None, group_number=None):
    """Update custom fields for an issue using direct field updates."""
    update_data = {"fields": {}}

    if hw_version:
        update_data["fields"][FIELD_HW_VERSION] = hw_version
    if group_number is not None:
        update_data["fields"][FIELD_GROUP_NUMBER] = str(group_number)

    if not update_data["fields"]:
        return True

    url = f"{jira_url}/rest/api/2/issue/{issue_key}"
    try:
        resp = requests.put(url, json=update_data, headers=jira_headers, timeout=30)
        resp.raise_for_status()
        logger.info(f"Updated custom fields for {issue_key}: HW={hw_version}, Group={group_number}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to update custom fields for {issue_key}: {e}")
        return False


def add_jira_comment(issue_key, comment_body):
    """Add comment to Jira issue."""
    comment_data = {"body": comment_body}
    url = f"{jira_url}/rest/api/2/issue/{issue_key}/comment"

    try:
        resp = requests.post(url, json=comment_data, headers=jira_headers, timeout=30)
        resp.raise_for_status()
        logger.info(f"Added comment to {issue_key}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to add comment to {issue_key}: {e}")
        return False


def update_issue_status(issue_key, target_status):
    """
    Update Jira issue status with proper transitions.
    target_status can be: 'In Progress', 'Done', 'Waiting for feedback'
    """
    transitions = {
        "In Progress": [{"id": TRANSITION_IN_PROGRESS}],  # To Do -> In Progress
        "Done": [{"id": TRANSITION_IN_PROGRESS}, {"id": TRANSITION_DONE}],  # To Do -> In Progress -> Done
        "Waiting for feedback": [{"id": TRANSITION_WAITING_FEEDBACK}]  # To Do -> Waiting for feedback
    }

    if target_status not in transitions:
        logger.error(f"Unknown target status: {target_status}")
        return False

    url = f"{jira_url}/rest/api/2/issue/{issue_key}/transitions"

    for transition in transitions[target_status]:
        transition_data = {"transition": transition}
        try:
            resp = requests.post(url, json=transition_data, headers=jira_headers, timeout=30)
            resp.raise_for_status()
            logger.info(f"Applied transition {transition['id']} to {issue_key}")
            time.sleep(1)  # Small delay between transitions
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to apply transition {transition['id']} to {issue_key}: {e}")
            return False

    logger.info(f"Updated {issue_key} status to {target_status}")
    return True


def send_email_notification(subject, body):
    """Send email notification for failed jobs (skipped if email is not configured)."""
    if not (EMAIL_USER and EMAIL_PASS and NOTIFICATION_EMAIL):
        logger.warning(f"Email not configured, skipping notification: {subject}")
        return False

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)

        message = f"Subject: {subject}\n\n{body}"
        server.sendmail(EMAIL_USER, NOTIFICATION_EMAIL, message)
        server.quit()

        logger.info(f"Email notification sent: {subject}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")
        return False


def send_dut_request(issue_data, target_node):
    """Send DUT request for an issue with specified target node."""
    tb_name = target_node  # Use the target node as TB name
    hw_version = issue_data.get('hw_version', 'Unknown')

    payload = [
        tb_name,
        DUT_CUSTOMER,
        hw_version,
        "",
        DUT_PROJECT,
        "New",
        NODE_ACTIVITY,
        "",
        DUT_KEY_TYPE,
        DUT_LOCATION,
        "None",
        "True"
    ]

    logger.info(f"Sending DUT request payload for issue {issue_data['key']} to node {target_node}: {payload}")

    try:
        resp = requests.post(dut_request_url, json=payload, headers={"Content-Type": "application/json"}, timeout=30)
        resp.raise_for_status()
        logger.info(f"DUT request sent successfully for issue {issue_data['key']} to node {target_node}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send DUT request for {issue_data['key']} to node {target_node}: {e}")
        return False


def poll_jira_for_dut_approval(issues_in_sw_group, timeout_minutes=10):
    """
    Poll Jira comments for DUT approval for a specific SW group.
    Returns node when found.
    """
    pattern = r"DUT request approved on node ([A-Za-z0-9\-_]+)"
    logger.info(f"Polling Jira comments for {len(issues_in_sw_group)} issues...")

    start_time = time.time()
    timeout_seconds = timeout_minutes * 60

    while time.time() - start_time < timeout_seconds:
        for issue in issues_in_sw_group:
            url = f"{jira_url}/rest/api/2/issue/{issue['key']}/comment"
            try:
                resp = requests.get(url, headers=jira_headers, timeout=30)
                resp.raise_for_status()
                comments = resp.json().get("comments", [])

                for c in comments:
                    match = re.search(pattern, c["body"])
                    if match:
                        node = match.group(1)
                        logger.info(f"DUT approval found: node={node} for issue {issue['key']}")
                        return node

            except requests.exceptions.RequestException as e:
                logger.warning(f"Error polling Jira comments for {issue['key']}: {e}")

        time.sleep(5)

    logger.warning(f"No DUT approval found after {timeout_minutes} minutes")
    return None


def get_next_build_number(job_name):
    """Get the next build number that will be assigned to a new build."""
    url = f"{jenkins_base_url}/job/{job_name}/api/json"
    try:
        resp = requests.get(url, headers=jenkins_headers_for_api, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        next_build = data.get("nextBuildNumber", 1)
        logger.info(f"Next build number for {job_name}: {next_build}")
        return next_build
    except Exception as e:
        logger.error(f"Failed to get next build number: {e}")
        return None


def poll_specific_build_until_done(job_name, build_number, poll_interval=10, timeout=36000):
    """
    Poll a specific Jenkins build number until it finishes.
    Returns dict with {building, result, url, parameters}.
    """
    start_time = time.time()
    url_build = f"{jenkins_base_url}/job/{job_name}/{build_number}/api/json"
    logger.info(f"Polling Jenkins job: {job_name} build #{build_number}")

    # Wait for build to appear
    time.sleep(10)

    while True:
        try:
            resp = requests.get(url_build, headers=jenkins_headers_for_api, timeout=30)

            if resp.status_code == 404:
                if time.time() - start_time > 120:  # Wait max 2 minutes for build to appear
                    logger.error(f"Build #{build_number} never appeared in Jenkins")
                    return {"building": False, "result": "NOT_FOUND", "url": None, "parameters": {}}
                time.sleep(poll_interval)
                continue

            resp.raise_for_status()
            data = resp.json()

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch build #{build_number} info: {e}")
            time.sleep(poll_interval)
            continue

        building = data.get("building", False)
        result = data.get("result") or ("RUNNING" if building else "UNKNOWN")
        build_url = data.get("url")

        parameters = {}
        for a in data.get("actions", []):
            for p in a.get("parameters", []):
                parameters[p["name"]] = p.get("value")

        logger.info(f"Build #{build_number} status: {result} ({'running' if building else 'finished'})")

        if not building:
            logger.info(f"Build #{build_number} finished with final status: {result}")
            return {"building": building, "result": result, "url": build_url, "parameters": parameters}

        if time.time() - start_time > timeout:
            logger.warning(f"Timeout reached waiting for build #{build_number}")
            return {"building": True, "result": "TIMEOUT", "url": build_url, "parameters": parameters}

        time.sleep(poll_interval)


def trigger_flashing_job_coordinated(node, sw, issues_for_sw, prefix, group_id):
    """Trigger flashing job with node coordination - only if node was successfully reserved."""
    thread_name = threading.current_thread().name
    logger.info(f"[{thread_name}] Starting coordinated flashing for SW {sw} on node {node} (Group {group_id})")

    # Check if this SW group has already failed flashing
    sw_group_key = f"{prefix}_{sw}"
    if sw_group_key in failed_sw_groups:
        logger.warning(f"[{thread_name}] SW group {sw_group_key} has already failed flashing, skipping")
        release_node(node)
        return False, None, None

    # Verify node is still reserved for us
    node_status = get_node_status(node)
    if node_status.get('status') != 'BUSY' or node_status.get('group_id') != group_id:
        logger.error(f"[{thread_name}] Node {node} is not properly reserved for group {group_id}, aborting flashing")
        return False, None, None

    try:
        next_build_number = get_next_build_number(FLASHING_JOB_NAME)
        if not next_build_number:
            logger.error(f"[{thread_name}] Could not get next build number for flashing")
            release_node(node)
            return False, None, None

        profile = get_sw_profile(sw)

        payload_flash = {
            "NODE_TO_RUN": node,
            "KEYWORD_VERSION": KEYWORD_VERSION,
            "SW_VERSION": SW_ARTIFACT_PATH_TEMPLATE.format(sw=sw),
            "FLASHING": "true",
            "HW_TYPE": HW_TYPE,
            "ECU_ENV": "env.conf",
            "BRANCH_CONFIG_TESTCASES": profile.get("branch_config_testcases", "UNKNOWN"),
            "DATABASE_PATH": profile.get("database_path", "UNKNOWN"),
            "PROGRAM": profile.get("program", "UNKNOWN"),
            "GENERATE_TAL": "true",
            "SW_BRANCH": "specific",
            "SPECIFIC_TAL_FILE_PARTIAL_NAME": "",
            "FLASH_BTLD": "true",
            "UPDATE_SE": "false"
        }

        logger.info(f"[{thread_name}] Sending flashing job payload: {payload_flash}")

        try:
            resp = requests.post(f"{jenkins_flashing_url}?token={jenkins_flashing_token}",
                                 json=payload_flash, headers=jenkins_headers, timeout=60)
            resp.raise_for_status()
            logger.info(f"[{thread_name}] Flashing triggered successfully, expected build #{next_build_number}")
        except Exception as e:
            logger.error(f"[{thread_name}] Failed to trigger flashing: {e}")
            release_node(node)
            return False, None, None

        build_info = poll_specific_build_until_done(FLASHING_JOB_NAME, next_build_number)

        # Comment on all issues in this SW group
        build_url = build_info.get('url', 'N/A')
        result = build_info.get('result', 'UNKNOWN')
        comment_body = f"Flashing build #{next_build_number} completed for SW {sw} on node {node}.\nStatus: {result}\nURL: {build_url}\nGenerated by Automation Script"

        for issue in issues_for_sw:
            add_jira_comment(issue['key'], comment_body)

        if result in ['ABORTED', 'FAILURE']:
            failed_sw_groups.add(sw_group_key)

            subject = f"Flashing Job Failed - SW {sw} on {node}"
            body = f"Flashing job has {result.lower()} for SW {sw} on node {node}.\n\nBuild URL: {build_url}"
            send_email_notification(subject, body)

            # Move all issues in this SW group to "Waiting for feedback" and track them
            for issue in issues_for_sw:
                issue_key = issue['key']
                update_issue_status(issue_key, "Waiting for feedback")
                failed_issue_keys.add(issue_key)
                waiting_feedback_issue_keys.add(issue_key)

            logger.error(f"[{thread_name}] Flashing failed for SW group {sw_group_key}, all issues moved to 'Waiting for feedback'")
            release_node(node)
            return False, build_info, next_build_number

        logger.info(f"[{thread_name}] Flashing successful for SW {sw}")
        # Note: Node is still reserved for execution phase
        return True, build_info, next_build_number

    except Exception as e:
        logger.error(f"[{thread_name}] Unexpected error during flashing: {e}")
        release_node(node)
        return False, None, None


def trigger_execution_job_coordinated(issue_data, sw, node, group_id):
    """Trigger execution job for a single issue with node coordination."""
    thread_name = threading.current_thread().name
    issue_key = issue_data["key"]
    branch = issue_data["branch"]
    testcases = issue_data["testcases"]
    sop = issue_data["sop"]
    type_value = issue_data["type"]
    domains = issue_data["domains"]

    logger.info(f"[{thread_name}] Starting coordinated execution for issue {issue_key}, SW {sw} on node {node} (Group {group_id})")

    # Verify node is still reserved for us (it should be after successful flashing)
    node_status = get_node_status(node)
    if node_status.get('status') != 'BUSY' or node_status.get('group_id') != group_id:
        logger.warning(f"[{thread_name}] Node {node} not properly reserved for group {group_id} during execution, proceeding anyway")

    try:
        next_build_number = get_next_build_number(EXECUTION_JOB_NAME)
        if not next_build_number:
            logger.error(f"[{thread_name}] Could not get next build number for execution")
            return False

        payload_exec = {
            "BRANCH": branch,
            "SW_VERSION": zero_pad_sw(sw),
            "TESTCASE_NAME": ",".join(testcases),
            "ACTIVITY": NODE_ACTIVITY,
            "KEYWORD_VERSION": KEYWORD_VERSION,
            "ECUMODE": "Engineering",
            "SOP": sop,
            "DOMAIN": domains,
            "TYPE": type_value
        }

        logger.info(f"[{thread_name}] Sending execution job payload for issue {issue_key}: {payload_exec}")

        try:
            resp = requests.post(f"{jenkins_base_url}/generic-webhook-trigger/invoke?token={execution_job_token}",
                                 json=payload_exec, headers=jenkins_headers, timeout=60)
            resp.raise_for_status()
            logger.info(f"[{thread_name}] Execution triggered successfully for {issue_key}, expected build #{next_build_number}")
        except Exception as e:
            logger.error(f"[{thread_name}] Failed to trigger execution for {issue_key}: {e}")
            return False

        build_info = poll_specific_build_until_done(EXECUTION_JOB_NAME, next_build_number)
        result = build_info.get('result', 'UNKNOWN')
        build_url = build_info.get('url', 'N/A')

        # Always comment on the issue regardless of result
        comment_body = f"Execution build #{next_build_number} completed for branch {branch} on node {node}.\nStatus: {result}\nURL: {build_url}\nGenerated by Automation Script"
        add_jira_comment(issue_key, comment_body)

        if result in ['ABORTED', 'FAILURE']:
            subject = f"Execution Job {result.title()} - Issue {issue_key}, Branch {branch}, SW {sw} on {node}"
            body = f"Execution job has {result.lower()} for issue {issue_key}, branch {branch}, SW {sw} on node {node}.\n\nBuild URL: {build_url}"
            send_email_notification(subject, body)

            update_issue_status(issue_key, "Waiting for feedback")
            failed_issue_keys.add(issue_key)
            waiting_feedback_issue_keys.add(issue_key)
        elif result in ['SUCCESS', 'UNSTABLE']:
            update_issue_status(issue_key, "Done")
            processed_issue_keys.add(issue_key)
            logger.info(f"[{thread_name}] Execution {result.lower()} for issue {issue_key} - moved to Done")

        return True

    except Exception as e:
        logger.error(f"[{thread_name}] Unexpected error during execution for {issue_key}: {e}")
        return False


def prepare_issue_data(issue):
    """Prepare individual issue data for execution using custom fields."""
    fields = issue["fields"]

    testcases_raw = fields.get(FIELD_TESTCASES, "")
    branch = fields.get(FIELD_BRANCH, "develop")
    sop_raw = fields.get(FIELD_SOP, "")
    sw_version = fields.get(FIELD_SW_VERSION, "")
    type_raw = fields.get(FIELD_TEST_BENCHES, "")  # Test benches field
    domains_raw = fields.get(FIELD_DOMAINS, "")  # Domains field
    description = fields.get("description", "")

    testcases = extract_testcases(testcases_raw)
    sop = extract_sop(sop_raw)
    type_value = extract_type(type_raw)

    # Try to extract domains from custom field first, then from description
    domains = extract_domains(domains_raw)
    if domains == DEFAULT_DOMAIN:  # Default value means no domain found in custom field
        domains = extract_domains_from_description(description)

    sw_bare = extract_sw_bare_format(sw_version)
    hw_version = get_hw_version_from_sw(sw_bare)

    logger.debug(f"Prepared issue data for {issue['key']}: testcases={testcases}, branch={branch}, sop={sop}, sw={sw_bare}, type={type_value}, domains={domains}")

    return {
        "key": issue["key"],
        "testcases": testcases,
        "branch": branch,
        "sop": sop,
        "sw_version": sw_bare,
        "type": type_value,
        "domains": domains,
        "hw_version": hw_version
    }


def assign_group_number_immediately(issue_key, prefix, group_numbers, next_group_number_ref):
    """
    Assign group number to an issue immediately when prefix is identified.
    Updates both the global group_numbers dict and the issue's custom field.
    Returns the assigned group number.
    """
    if prefix not in group_numbers:
        group_numbers[prefix] = next_group_number_ref[0]
        logger.info(f"Assigned group number {next_group_number_ref[0]} to new SW prefix {prefix}")
        next_group_number_ref[0] += 1

    group_number = group_numbers[prefix]

    try:
        url = f"{jira_url}/rest/api/2/issue/{issue_key}?fields={FIELD_SW_VERSION}"
        resp = requests.get(url, headers=jira_headers, timeout=30)
        resp.raise_for_status()
        issue_data = resp.json()

        sw_version = issue_data["fields"].get(FIELD_SW_VERSION, "")
        sw_bare = extract_sw_bare_format(sw_version)
        hw_version = get_hw_version_from_sw(sw_bare)

        update_issue_custom_fields(issue_key, hw_version, group_number)
        return group_number

    except Exception as e:
        logger.error(f"Failed to assign group number to {issue_key}: {e}")
        return group_number


def process_sw_in_group_sequential_coordinated(prefix, sorted_sw_versions, sw_groups, reserved_node, group_id, skip_flashing_for_exact=False, selection_type="unknown"):
    """Process SW versions sequentially within a prefix group with coordinated node management."""
    thread_name = threading.current_thread().name
    logger.info(f"[{thread_name}] Processing prefix group {prefix} sequentially on RESERVED node {reserved_node} (Group {group_id})")
    logger.info(f"[{thread_name}] Processing order: {sorted_sw_versions}")

    try:
        for sw in sorted_sw_versions:
            issues_for_sw = sw_groups[sw]
            logger.info(f"[{thread_name}] Processing SW {sw} ({len(issues_for_sw)} issues) in sequential order")

            sw_group_key = f"{prefix}_{sw}"
            has_failed_issues = any(issue['key'] in failed_issue_keys for issue in issues_for_sw)
            has_waiting_issues = any(issue['key'] in waiting_feedback_issue_keys for issue in issues_for_sw)

            if sw_group_key in failed_sw_groups or has_failed_issues or has_waiting_issues:
                logger.warning(f"[{thread_name}] SW group {sw} has failed issues or waiting feedback, skipping")
                continue

            # Step 1: Trigger flashing only if not skipping (node is already reserved)
            flashing_success = True
            if not skip_flashing_for_exact:
                flashing_success, build_info, build_number = trigger_flashing_job_coordinated(reserved_node, sw, issues_for_sw, prefix, group_id)
                if not flashing_success:
                    logger.error(f"[{thread_name}] Flashing failed for SW {sw}, stopping sequential processing for this prefix group")
                    break  # Stop processing remaining SW versions in this prefix group
                logger.info(f"[{thread_name}] Flashing successful for SW {sw}, starting individual execution phase")
            else:
                logger.info(f"[{thread_name}] Skipping flashing for SW {sw} (exact match), proceeding to execution")

            # Step 2: Execute each issue individually only if flashing was successful
            if flashing_success:
                for issue in issues_for_sw:
                    if issue['key'] in processed_issue_keys or issue['key'] in failed_issue_keys:
                        logger.debug(f"[{thread_name}] Issue {issue['key']} already processed/failed, skipping")
                        continue

                    issue_data = prepare_issue_data(issue)
                    logger.info(f"[{thread_name}] Executing issue {issue['key']} for SW {sw}")
                    success = trigger_execution_job_coordinated(issue_data, sw, reserved_node, group_id)

                    if not success:
                        logger.error(f"[{thread_name}] Execution failed for issue {issue['key']}")

                    time.sleep(5)  # Small delay between individual executions

        logger.info(f"[{thread_name}] Completed sequential processing of prefix group {prefix}")

    finally:
        # Always release the node when done with sequential processing
        release_node(reserved_node)
        logger.info(f"[{thread_name}] Released node {reserved_node} after sequential processing")


def process_sw_in_group_coordinated(group_id, sw, issues_for_sw, bts_nodes, prefix):
    """Process a specific SW within a group with coordinated 3-tier node selection logic."""
    thread_name = threading.current_thread().name
    logger.info(f"[{thread_name}] Processing SW {sw} in group {group_id} with coordination")

    sw_group_key = f"{prefix}_{sw}"
    if sw_group_key in failed_sw_groups:
        logger.warning(f"[{thread_name}] SW group {sw_group_key} has already failed, skipping")
        return False

    has_failed_issues = any(issue['key'] in failed_issue_keys for issue in issues_for_sw)
    has_waiting_issues = any(issue['key'] in waiting_feedback_issue_keys for issue in issues_for_sw)

    if has_failed_issues or has_waiting_issues:
        logger.warning(f"[{thread_name}] SW {sw} has failed issues or waiting feedback, skipping")
        return False

    selected_node, selection_type, skip_flashing, reserved = find_node_with_3_tier_selection_coordinated(sw, bts_nodes, group_id, prefix)

    if not reserved:
        logger.error(f"[{thread_name}] Could not reserve node {selected_node} for SW {sw}, skipping")
        return False

    try:
        # Step 1: Trigger flashing only if not skipping
        if not skip_flashing:
            flashing_success, build_info, build_number = trigger_flashing_job_coordinated(selected_node, sw, issues_for_sw, prefix, group_id)
            if not flashing_success:
                logger.error(f"[{thread_name}] Flashing failed for SW {sw}, skipping execution")
                return False
            logger.info(f"[{thread_name}] Flashing successful for SW {sw}, starting individual execution phase")
        else:
            logger.info(f"[{thread_name}] Skipping flashing for SW {sw} (selection_type: {selection_type}), proceeding to execution")

        # Step 2: Execute each issue individually
        for issue in issues_for_sw:
            if issue['key'] in processed_issue_keys or issue['key'] in failed_issue_keys:
                logger.debug(f"[{thread_name}] Issue {issue['key']} already processed/failed, skipping")
                continue

            issue_data = prepare_issue_data(issue)
            logger.info(f"[{thread_name}] Executing issue {issue['key']} for SW {sw}")
            success = trigger_execution_job_coordinated(issue_data, sw, selected_node, group_id)

            if not success:
                logger.error(f"[{thread_name}] Execution failed for issue {issue['key']}")

            time.sleep(5)  # Small delay between individual executions

        logger.info(f"[{thread_name}] Completed processing SW {sw} in group {group_id}")
        return True

    finally:
        # Always release the node
        release_node(selected_node)


def process_new_group_enhanced_coordinated(group_id, prefix, issues_in_group, bts_nodes):
    """
    Process a new unprocessed group with batch node selection, sequential SW
    processing, and node coordination.

    Node selection tiers:
      1. Exact software match
      2. Prefix-compatible node
      3. Any available execution node
    """
    thread_name = threading.current_thread().name
    logger.info(f"[{thread_name}] Processing new group {group_id} (prefix: {prefix}) with coordinated tiered logic")

    unprocessed_issues = [
        issue for issue in issues_in_group
        if issue['key'] not in processed_issue_keys
        and issue['key'] not in failed_issue_keys
        and issue['key'] not in waiting_feedback_issue_keys
    ]

    if not unprocessed_issues:
        logger.info(f"[{thread_name}] All issues in group {group_id} already processed/failed/waiting, skipping")
        return

    # Group issues by their exact SW (bare format)
    sw_groups = defaultdict(list)
    for issue in unprocessed_issues:
        fields = issue["fields"]
        sw_version = fields.get(FIELD_SW_VERSION, "")
        sw = extract_sw_bare_format(sw_version)
        if sw:
            sw_groups[sw].append(issue)

    all_sw_versions = list(sw_groups.keys())
    sorted_sw_versions = sort_sw_versions_ascending(all_sw_versions)

    logger.info(f"[{thread_name}] Group {group_id} has {len(sw_groups)} distinct SWs in ascending order: {sorted_sw_versions}")

    # ===== TIER 1: Batch Exact Match Detection with Coordination =====
    logger.info(f"[{thread_name}] TIER 1: Scanning for available exact matches across entire prefix group")

    exact_matches = {}  # sw_version -> node_name (only if available)
    available_qc_execution_nodes = []
    available_prefix_compatible_nodes = {}  # node_name -> node_sw (only if available)

    for node in bts_nodes:
        node_sw_raw = node.get('sw', '')
        node_sw = normalize_sw_version(node_sw_raw)
        node_activity = node.get('activity', '')
        node_name = node['bts_name']

        node_status = get_node_status(node_name)
        if node_status['status'] != 'FREE':
            logger.debug(f"[{thread_name}] Node {node_name} is busy (Group {node_status.get('group_id', 'UNKNOWN')}), skipping")
            continue

        logger.debug(f"[{thread_name}] Analyzing available node {node_name}: SW={node_sw}, Activity={node_activity}")

        if node_activity == NODE_ACTIVITY:
            available_qc_execution_nodes.append(node_name)

            # Check if node SW belongs to same prefix group
            if node_sw:
                node_prefix = extract_sw_prefix(node_sw)
                if node_prefix == prefix:
                    available_prefix_compatible_nodes[node_name] = node_sw
                    logger.debug(f"[{thread_name}] Node {node_name} is available and prefix-compatible (prefix: {node_prefix})")

            # Check for exact matches with any SW in the group
            for target_sw in all_sw_versions:
                if node_sw == target_sw:
                    exact_matches[target_sw] = node_name
                    logger.info(f"[{thread_name}] TIER 1 AVAILABLE EXACT MATCH: SW {target_sw} -> Node {node_name}")

    logger.info(f"[{thread_name}] Tier 1 Results:")
    logger.info(f"[{thread_name}]   - Available exact matches: {exact_matches}")
    logger.info(f"[{thread_name}]   - Available execution nodes: {available_qc_execution_nodes}")
    logger.info(f"[{thread_name}]   - Available prefix-compatible nodes: {available_prefix_compatible_nodes}")

    processed_sw_versions = set()

    # ===== TIER 1: Process Available Exact Matches Immediately =====
    if exact_matches:
        logger.info(f"[{thread_name}] TIER 1: Processing {len(exact_matches)} available exact matches immediately")

        for sw_version, node_name in exact_matches.items():
            if sw_version in processed_sw_versions:
                continue

            if not reserve_node_for_sw(node_name, sw_version, group_id, prefix):
                logger.warning(f"[{thread_name}] Could not reserve exact match node {node_name} for SW {sw_version} (may have been taken by another thread)")
                continue

            issues_for_sw = sw_groups[sw_version]
            logger.info(f"[{thread_name}] TIER 1: Processing SW {sw_version} on reserved exact match node {node_name}")

            try:
                # Post the approval comment (simulates an instant DUT approval)
                random_issue = random.choice(issues_for_sw)
                comment_body = f"DUT request approved on node {node_name}"
                add_jira_comment(random_issue['key'], comment_body)
                logger.info(f"[{thread_name}] Added approval comment to {random_issue['key']}")

                # Execute directly since node is reserved and SW matches (skip flashing)
                for issue in issues_for_sw:
                    if issue['key'] in processed_issue_keys or issue['key'] in failed_issue_keys:
                        continue

                    issue_data = prepare_issue_data(issue)
                    logger.info(f"[{thread_name}] TIER 1: Executing issue {issue['key']} for SW {sw_version}")
                    success = trigger_execution_job_coordinated(issue_data, sw_version, node_name, group_id)

                    if success:
                        logger.info(f"[{thread_name}] TIER 1: Successfully executed issue {issue['key']}")
                    else:
                        logger.error(f"[{thread_name}] TIER 1: Failed to execute issue {issue['key']}")
                    time.sleep(5)  # Small delay between individual executions

                processed_sw_versions.add(sw_version)

            finally:
                release_node(node_name)

    # ===== TIER 2: Prefix-Compatible Node Sequential Processing with Coordination =====
    remaining_sorted_sw_versions = [sw for sw in sorted_sw_versions if sw not in processed_sw_versions]

    if available_prefix_compatible_nodes and remaining_sorted_sw_versions:
        logger.info(f"[{thread_name}] TIER 2: Found available prefix-compatible nodes, attempting sequential processing")

        selected_node = None
        for node_name, node_sw in available_prefix_compatible_nodes.items():
            if reserve_node_for_sw(node_name, f"sequential_{prefix}", group_id, prefix, "SEQUENTIAL"):
                selected_node = node_name
                logger.info(f"[{thread_name}] TIER 2: Reserved prefix-compatible node {selected_node} (SW: {node_sw}) for sequential processing")
                break

        if selected_node:
            first_sw = remaining_sorted_sw_versions[0]
            representative_issue = random.choice(sw_groups[first_sw])
            comment_body = f"DUT request approved on node {selected_node}"
            add_jira_comment(representative_issue['key'], comment_body)
            logger.info(f"[{thread_name}] TIER 2: Added approval comment for prefix-compatible node {selected_node}")

            remaining_sw_groups = {sw: sw_groups[sw] for sw in remaining_sorted_sw_versions}

            process_sw_in_group_sequential_coordinated(prefix, remaining_sorted_sw_versions, remaining_sw_groups, selected_node, group_id,
                                                       skip_flashing_for_exact=False, selection_type="prefix_compatible")

            processed_sw_versions.update(remaining_sorted_sw_versions)
            remaining_sorted_sw_versions = []
        else:
            logger.warning(f"[{thread_name}] TIER 2: Could not reserve any prefix-compatible nodes, falling to lower tiers")

    # ===== TIER 3: Any Execution Node with Coordination =====
    if available_qc_execution_nodes and remaining_sorted_sw_versions:
        logger.info(f"[{thread_name}] TIER 3: Processing remaining SW versions with available execution nodes")

        selected_node = None
        for node_name in available_qc_execution_nodes:
            if node_name not in available_prefix_compatible_nodes:  # Avoid double-trying prefix-compatible nodes
                if reserve_node_for_sw(node_name, f"sequential_{prefix}", group_id, prefix, "SEQUENTIAL"):
                    selected_node = node_name
                    logger.info(f"[{thread_name}] TIER 3: Reserved execution node {selected_node} for sequential processing")
                    break

        if selected_node:
            first_sw = remaining_sorted_sw_versions[0]
            issues_for_sw = sw_groups[first_sw]

            logger.info(f"[{thread_name}] TIER 3: Processing remaining SWs starting with {first_sw} on reserved execution node {selected_node}")

            representative_issue = random.choice(issues_for_sw)
            issue_data = prepare_issue_data(representative_issue)

            logger.info(f"[{thread_name}] TIER 3: Sending DUT request for SW {first_sw} using issue {representative_issue['key']}")
            send_dut_request(issue_data, selected_node)

            logger.info(f"[{thread_name}] TIER 3: Waiting for DUT approval")
            approved_node = poll_jira_for_dut_approval(issues_for_sw, timeout_minutes=10)

            if approved_node and approved_node == selected_node:
                logger.info(f"[{thread_name}] TIER 3: DUT approved on reserved node {approved_node}")

                remaining_sw_groups = {sw: sw_groups[sw] for sw in remaining_sorted_sw_versions}

                process_sw_in_group_sequential_coordinated(prefix, remaining_sorted_sw_versions, remaining_sw_groups, approved_node, group_id,
                                                           skip_flashing_for_exact=False, selection_type="qc_execution_only")

                processed_sw_versions.update(remaining_sorted_sw_versions)
                remaining_sorted_sw_versions = []
            else:
                logger.error(f"[{thread_name}] TIER 3: No DUT approval found or approved on different node")
                release_node(selected_node)  # Release since we couldn't use it
        else:
            logger.warning(f"[{thread_name}] TIER 3: Could not reserve any execution nodes")

    logger.info(f"[{thread_name}] Group {group_id} processing completed. Processed SW versions: {list(processed_sw_versions)}")


def process_processed_group_coordinated(group_id, prefix, issues_in_group, bts_nodes):
    """Process new tickets joining an already processed group with coordinated 3-tier selection."""
    thread_name = threading.current_thread().name
    logger.info(f"[{thread_name}] Processing new tickets in already processed group {group_id} (prefix: {prefix}) with coordination")

    unprocessed_issues = [
        issue for issue in issues_in_group
        if issue['key'] not in processed_issue_keys
        and issue['key'] not in failed_issue_keys
        and issue['key'] not in waiting_feedback_issue_keys
    ]

    if not unprocessed_issues:
        logger.info(f"[{thread_name}] All issues in group {group_id} already processed/failed/waiting, skipping")
        return

    sw_groups = defaultdict(list)
    for issue in unprocessed_issues:
        fields = issue["fields"]
        sw_version = fields.get(FIELD_SW_VERSION, "")
        sw = extract_sw_bare_format(sw_version)
        if sw:
            sw_groups[sw].append(issue)

    logger.info(f"[{thread_name}] Processed group {group_id} has {len(sw_groups)} distinct SWs with new tickets: {list(sw_groups.keys())}")

    for sw, issues_for_sw in sw_groups.items():
        sw_group_key = f"{prefix}_{sw}"
        has_failed_issues = any(issue['key'] in failed_issue_keys for issue in issues_for_sw)
        has_waiting_issues = any(issue['key'] in waiting_feedback_issue_keys for issue in issues_for_sw)

        if sw_group_key in failed_sw_groups or has_failed_issues or has_waiting_issues:
            logger.warning(f"[{thread_name}] SW group {sw} has failed issues or waiting feedback, skipping")
            continue

        success = process_sw_in_group_coordinated(group_id, sw, issues_for_sw, bts_nodes, prefix)
        if success:
            logger.info(f"[{thread_name}] Successfully processed SW {sw} in processed group {group_id}")
        else:
            logger.warning(f"[{thread_name}] Failed to process SW {sw} in processed group {group_id}")


def process_group_enhanced_coordinated(group_id, prefix, issues_in_group):
    """Process a group using coordinated tiered batch processing logic."""
    thread_name = threading.current_thread().name
    logger.info(f"[{thread_name}] Starting coordinated enhanced processing of group {group_id} (prefix: {prefix})")

    if group_id not in group_locks:
        group_locks[group_id] = threading.Lock()

    with group_locks[group_id]:
        unprocessed_issues = [
            issue for issue in issues_in_group
            if issue['key'] not in processed_issue_keys
            and issue['key'] not in failed_issue_keys
            and issue['key'] not in waiting_feedback_issue_keys
        ]

        if not unprocessed_issues:
            logger.info(f"[{thread_name}] All issues in group {group_id} already processed/failed/waiting, skipping")
            return

        group_processing_status[group_id] = "PROCESSING"

        try:
            bts_nodes = get_bts_nodes()
            if not bts_nodes:
                logger.warning(f"[{thread_name}] Could not get node information")
                group_processing_status[group_id] = "ERROR"
                return

            all_group_issues = issues_in_group
            any_processed = any(issue['key'] in processed_issue_keys for issue in all_group_issues)

            if any_processed:
                logger.info(f"[{thread_name}] Group {group_id} has some processed issues, using processed group logic")
                process_processed_group_coordinated(group_id, prefix, unprocessed_issues, bts_nodes)
            else:
                logger.info(f"[{thread_name}] Group {group_id} is completely new, using enhanced new group logic")
                process_new_group_enhanced_coordinated(group_id, prefix, unprocessed_issues, bts_nodes)

            group_processing_status[group_id] = "COMPLETED"
            logger.info(f"[{thread_name}] Coordinated enhanced group {group_id} processing completed")

        except Exception as e:
            logger.error(f"[{thread_name}] Error in coordinated enhanced processing of group {group_id}: {e}")
            group_processing_status[group_id] = "ERROR"


def cleanup_stale_group_processing():
    """Clean up stale group processing statuses."""
    # Simple placeholder: a full implementation would track timestamps per group.
    logger.debug("Group processing cleanup completed")


def log_system_status():
    """Log current system status for debugging."""
    thread_name = threading.current_thread().name
    logger.info(f"[{thread_name}] === SYSTEM STATUS ===")
    logger.info(f"[{thread_name}] Processed issues: {len(processed_issue_keys)}")
    logger.info(f"[{thread_name}] Failed issues: {len(failed_issue_keys)}")
    logger.info(f"[{thread_name}] Waiting feedback issues: {len(waiting_feedback_issue_keys)}")
    logger.info(f"[{thread_name}] Failed SW groups: {len(failed_sw_groups)}")

    all_node_statuses = get_all_node_statuses()
    busy_nodes = {node: status for node, status in all_node_statuses.items() if status['status'] == 'BUSY'}
    logger.info(f"[{thread_name}] Currently busy nodes: {len(busy_nodes)}")
    for node, status in busy_nodes.items():
        logger.info(f"[{thread_name}]   - {node}: SW {status['sw_version']} (Group {status['group_id']}, Thread {status['thread_name']})")

    active_groups = {gid: status for gid, status in group_processing_status.items() if status == "PROCESSING"}
    logger.info(f"[{thread_name}] Currently processing groups: {len(active_groups)}")
    for group_id, status in active_groups.items():
        logger.info(f"[{thread_name}]   - Group {group_id}: {status}")


def main():
    """Main execution loop with coordinated node management."""
    validate_config()
    logger.info("Starting Jira-Jenkins automation script with coordinated node management...")

    group_numbers = {}
    next_group_number = [1]  # Use list to make it mutable for the helper function

    with ThreadPoolExecutor(max_workers=10, thread_name_prefix="GroupProcessor") as executor:
        cycle_count = 0
        while True:
            try:
                cycle_count += 1
                logger.info(f"\n===== Starting cycle #{cycle_count} at {time.strftime('%Y-%m-%d %H:%M:%S')} =====")

                # Periodic cleanup of stale reservations and group processing
                if cycle_count % 10 == 0:
                    cleanup_stale_reservations(max_age_hours=2)
                    cleanup_stale_group_processing()

                check_if_issue_in_waiting_feedback()

                issues = get_jira_issues()
                logger.info(f"Found {len(issues)} Test Execution issues")

                if not issues:
                    logger.info("No issues found, sleeping...")
                    time.sleep(120)
                    continue

                if issues:
                    logger.debug(f"First issue keys: {list(issues[0].keys())}")
                    if 'fields' in issues[0]:
                        logger.debug(f"Available custom fields in first issue: {list(issues[0]['fields'].keys())}")
                    else:
                        logger.error(f"No 'fields' key found. Full issue structure: {issues[0]}")

                # First pass: immediate group number assignment for all issues
                for issue in issues:
                    if "fields" not in issue:
                        logger.error(f"Issue {issue.get('key', 'UNKNOWN')} has no 'fields' key")
                        continue

                    fields = issue["fields"]
                    sw_version = fields.get(FIELD_SW_VERSION, "")
                    if not sw_version:
                        continue

                    sw = extract_sw_bare_format(sw_version)
                    if not sw:
                        continue

                    prefix = extract_sw_prefix(sw)
                    if not prefix:
                        continue

                    current_group = fields.get(FIELD_GROUP_NUMBER)
                    expected_group = group_numbers.get(prefix)

                    current_group_str = str(current_group) if current_group is not None else None
                    expected_group_str = str(expected_group) if expected_group is not None else None

                    if expected_group is None or current_group_str != expected_group_str:
                        assigned_group = assign_group_number_immediately(issue['key'], prefix, group_numbers, next_group_number)
                        logger.info(f"Assigned/updated group {assigned_group} for issue {issue['key']} with SW prefix {prefix}")

                # Second pass: group issues for processing
                sw_groups = {}
                for issue in issues:
                    if "fields" not in issue:
                        continue

                    fields = issue["fields"]
                    sw_version = fields.get(FIELD_SW_VERSION, "")
                    if not sw_version:
                        continue

                    sw = extract_sw_bare_format(sw_version)
                    if not sw:
                        continue

                    prefix = extract_sw_prefix(sw)
                    if not prefix:
                        continue

                    sw_groups.setdefault(prefix, []).append(issue)

                logger.info(f"Created {len(sw_groups)} SW groups: {list(sw_groups.keys())}")

                # Third pass: log current tracking status
                if cycle_count % 5 == 0:
                    log_system_status()

                # Submit groups for parallel processing
                future_to_group = {}
                for prefix, issues_in_group in sw_groups.items():
                    group_id = group_numbers[prefix]

                    unprocessed_issues = [
                        issue for issue in issues_in_group
                        if issue['key'] not in processed_issue_keys
                        and issue['key'] not in failed_issue_keys
                        and issue['key'] not in waiting_feedback_issue_keys
                    ]

                    if not unprocessed_issues or group_processing_status.get(group_id) == "PROCESSING":
                        if not unprocessed_issues:
                            logger.debug(f"Group {group_id} has no unprocessed issues, skipping")
                        else:
                            logger.debug(f"Group {group_id} is currently processing, skipping")
                        continue

                    logger.info(f"Submitting group {group_id} (prefix: {prefix}) with {len(unprocessed_issues)} unprocessed issues for coordinated processing")
                    future = executor.submit(process_group_enhanced_coordinated, group_id, prefix, issues_in_group)
                    future_to_group[future] = (group_id, prefix)

                logger.info(f"Submitted {len(future_to_group)} groups for parallel coordinated processing")

                logger.info("Cycle completed, sleeping for 2 minutes...")
                time.sleep(120)

            except KeyboardInterrupt:
                logger.info("Script interrupted by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                logger.info("Sleeping for 60 seconds before retrying...")
                time.sleep(60)


if __name__ == "__main__":
    main()
