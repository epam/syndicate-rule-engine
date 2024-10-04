#!/bin/bash

set -eo pipefail

cmd_usage() {
  cat <<EOF
Manage Rule Engine installation

Usage:
  $PROGRAM [command]

Available Commands:
  backup   Allow to manage backups
  help     Show help message
  health   Check installation health
  init     Initialize Rule Engine installation
  list     Lists available updates
  nginx    Allow to enable and disable nginx sites
  secrets  Allow to retrieve some secrets generated on startup. They are located inside k8s cluster
  update   Update the installation
  version  Print versions information
EOF
}

cmd_init_usage() {
  cat <<EOF
Initializes Rule Engine

Description:
  Initializes Rule Engine for the first time or for a specified user. Includes installing CLIs, configuring passwords and other

Usage:
  $PROGRAM $COMMAND [options]

Examples:
  $PROGRAM $COMMAND --system
  $PROGRAM $COMMAND --user example --public-ssh-key "ssh-rsa AAA..."

Options:
  -h, --help        Show this message and exit
  --system          Initialize SRE for the first time. Only possible for $FIRST_USER user. Creates necessary system entities
  --user            Initialize SRE for the given user
  --public-ssh-key  If specified will be added to user's authorized_keys.
  --re-username     Rule Engine username to configure. Must be specified together with --re-password
  --re-password     Rule Engine password to configure. Must be specified together with --re-username
  --admin-username  Modular Service username to configure. Must be specified together with --admin-password
  --admin-password  Modular Service password to configure. Must be specified together with --admin-username
EOF
}

cmd_update_usage() {
  cat <<EOF
Updates local Rule Engine Installation

Description:
  Checks for new release and performs update if it's available

Usage:
  $PROGRAM $COMMAND [options]

Examples:
  $PROGRAM $COMMAND --check
  $PROGRAM $COMMAND -y

Options:
  --backup-name        Backup name to make before the update (default "$AUTO_BACKUP_PREFIX\$timestamp")
  --check              Checks whether update is available but do not try to update
  --no-backup          Do not do backup
  -h, --help           Show this message and exit
  -y, --yes            Automatic yes to prompts
EOF
}
cmd_update_list_usage() {
  cat <<EOF
Displays available releases

Description:
  List only new available Rule Engine releases and the current one. Uses GitHub rest api under the hood and
  can throttle if rate limit is exceeded

Usage:
  $PROGRAM $COMMAND [options]

Examples:
  $PROGRAM $COMMAND

Options:
  -h, --help           Show this message and exit
EOF
}

cmd_nginx_usage() {
  cat <<EOF
Manage existing nginx configurations for Rule Engine

Description:
  Allows to enable and disable existing nginx configurations and corresponding k8s services. The command is not designed
  to be flexible. It just allows to enable and disable pre-defined services easily.

Examples:
  $PROGRAM $COMMAND ls
  $PROGRAM $COMMAND enable sre
  $PROGRAM $COMMAND disable defectdojo

Available Commands:
  disable     Disable the given nginx server
  enable      Enable the given nginx server
  help        Show help message
  ls          Show available nginx servers

Options:
  -h, --help  Show this message and exit
EOF
}

cmd_backup_usage() {
  cat <<EOF
Manage local backups

Description:
  Command for managing local backups of persistent volumes from k8s

Examples:
  $PROGRAM $COMMAND

Available Command:
  create      Creates a new backup
  help        Show help message
  ls          Show created backups
  restore     Restores backup
  rm          Removes existing backup

Options
  -h, --help  Show helm message
EOF
}

cmd_backup_list_usage() {
  cat <<EOF
Shows local backups

Description:
  Command for describing all created backups

Examples:
  $PROGRAM $COMMAND ls

Options
  -h, --help     Show help message
  -v, --version  Version of Rule Engine release for which backups where made (default current release "$(get_helm_release_version "$HELM_RELEASE_NAME")")
  -p, --path     Path where backups are store (default "$SRE_BACKUPS_PATH/\$version"). --version parameter is ignored when custom --path is specified
EOF
}
cmd_backup_rm_usage() {
  cat <<EOF
Removes local backup

Description:
  Command for removing local backup

Examples:
  $PROGRAM $COMMAND rm --name my-backup

Required Options:
  -n, --name     Backup name to remove

Options
  -h, --help     Show help message
  -y, --yes      Automatic yes to prompts
  -v, --version  Version of Rule Engine release for which backups where made (default current release "$(get_helm_release_version "$HELM_RELEASE_NAME")")
  -p, --path     Path where backups are stored (default "$SRE_BACKUPS_PATH/\$version"). Note that --version parameter is ignored when custom --path is specified
EOF
}
cmd_backup_create_usage() {
  cat <<EOF
Creates local backup

Description:
  Command for creating local backup

Examples:
  $PROGRAM $COMMAND create --name my-backup
  $PROGRAM $COMMAND create --name my-backup --volumes=minio,mongo,vault

Required Options:
  -n, --name  Backup name to create

Options
  -h, --help  Show help message
  -p, --path  Path where backups are store (default "$SRE_BACKUPS_PATH/$(get_helm_release_version "$HELM_RELEASE_NAME")")
  --volumes   Volumes to make the backup for. Uses all k8s volumes if not specified. Specify volumes divided by comma
EOF
}

cmd_backup_restore_usage() {
  cat <<EOF
Restores local backup

Description:
  Command for restoring local backup

Examples:
  $PROGRAM $COMMAND restore --name my-backup
  $PROGRAM $COMMAND restore --name my-backup --volumes=minio,mongo,vault

Required Options:
  -n, --name     Backup name to create

Options
  -h, --help     Show help message
  -v, --version  Version of Rule Engine release for which backups where made (default current release "$(get_helm_release_version "$HELM_RELEASE_NAME")")
  -p, --path     Path where backups are store (default "$SRE_BACKUPS_PATH/\$version"). --version parameter is ignored when custom --path is specified
  -f, --force    Restore backup even if current release version does not match to the release version where backup was made
  --volumes      Volumes to make the backup for. Uses all k8s volumes if not specified. Specify volumes divided by comma
EOF
}

cmd_health_usage() {
  cat <<EOF
Checks installation health

Description:
  Command that verifies different aspects of installation

Examples:
  $PROGRAM $COMMAND

Options
  -h, --help  Show helm message
EOF
}

cmd_version() {
  echo "$PROGRAM: $VERSION"
}
die() { echo "Error:" "$@" >&2; exit 1; }
warn() { echo "Warning:" "$@" >&2; }
cmd_unrecognized() {
  cat <<EOF
Error: unrecognized command \`$PROGRAM $COMMAND\`
Try '$PROGRAM --help' for more information
EOF
}

# helper functions
get_latest_local_release() { ls "$SRE_RELEASES_PATH" | sort -r | head -n 1; }
get_helm_release_version() {
  # currently the version of rule engine chart corresponds to the version of app inside
  helm get metadata "$1" -o json 2>/dev/null | jq -r '.version'
}

# some github api functions
iter_github_releases() {
  # iterates only over released version by default. --prerelease flag includes pre-releases to output. --draft includes drafts
  local opts draft=0 prerelease=0 per_page=${GITHUB_PER_PAGE:-30} filter
  opts="$(getopt -o "" --long "draft,prerelease,per-page:," -n iter_github_releases -- "$@")"
  eval set -- "$opts"
  while true; do
    case "$1" in
      --draft) draft=1; shift ;;
      --prerelease) prerelease=1; shift ;;
      --per-page) per_page="$2"; shift 2 ;;
      '--') shift; break ;;
    esac
  done
  # todo couldn't think of other solution fast
  if [ "$draft" -eq 1 ] && [ "$prerelease" -eq 1 ]; then
    filter='.[]'
  elif [ "$draft" -eq 0 ] && [ "$prerelease" -eq 1 ]; then
    filter='.[] | select(.draft == false)'
  elif [ "$draft" -eq 1 ] && [ "$prerelease" -eq 0 ]; then
    filter='.[] | select(.prerelease == false)'
  else  # both 0
    filter='.[] | select(.prerelease == false and .draft == false)'
  fi
  # todo add pagination if needed
  curl -fLs --request GET -H 'Accept: application/vnd.github+json' "${GITHUB_CURL_HEADERS[@]}" "https://api.github.com/repos/$GITHUB_REPO/releases?per_page=$per_page" | jq -c "$filter" || die "Could not make another request to GitHub. Probably rate limit exceeded"
}

get_github_release_by_tag() {
  local tag_name
  while IFS= read -r item; do
    tag_name=$(jq -r '.tag_name' <<<"$item")
    if [ "$1" = "$tag_name" ]; then
      echo "$item"
      return
    fi
  done < <(iter_github_releases --prerelease --draft)
  return 1
}

get_new_github_release() {
  # requires one parameter -> current release
  local current_release="$1" tag_name result
  shift # all other parameter are passed to iter_github_releases

  while IFS= read -r item; do
    tag_name=$(jq -r '.tag_name' <<<"$item")
    if [[ "$current_release" < "$tag_name" ]]; then
      result="$item"
    elif [ -n "$result" ]; then  # higher or equal
      echo "$result"
      return 0
    else
      break
    fi
  done < <(iter_github_releases "$@")
  return 1
}

# shellcheck disable=SC2120
generate_password() {
  chars="20"
  typ='-base64'
  if [ -n "$1" ]; then
    chars="$1"
  fi
  if [ -n "$2" ]; then
    typ="$2"
  fi
  openssl rand "$typ" "$chars"
}
get_imds_token () {
  curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 20"
}
account_id() { curl -s curl -s -H "X-aws-ec2-metadata-token: $(get_imds_token)" http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r ".accountId"; }
user_exists() { id "$1" &>/dev/null; }
get_kubectl_secret() {
  kubectl get secret "$1" -o jsonpath="{.data.$2}" | base64 --decode
}
yesno() {
	[[ -t 0 ]] || return 0
	local response
	read -r -p "$1 [y/N] " response
	[[ $response == [yY] ]] || exit 1
}
colorize() {
  local color
  case "$1" in
    GREEN) color="\033[0;32m" ;;
    RED) color="\033[31m" ;;
    YELLOW) color="\033[0;33m" ;;
    *) color=""
  esac
  printf "%b" "$color"
  cat -
  printf "\033[0m"
}
patch_kubectl_secret() {
  local secret
  secret=$(base64 <<< "$3")
  kubectl patch secret "$1" -p="{\"data\":{\"$2\":\"$secret\"}}"
}

get_account_alias() {
  local output
  if output="$(aws iam list-account-aliases 2>&1)"; then
    jq -r '.AccountAliases[0]' <<<"$output"
  fi
}

resolve_tenant_name() {
  if [ -n "$TENANT_NAME" ]; then
    echo "${TENANT_NAME^^}"
    return
  fi
  local tn
  tn="$(get_account_alias)"
  if [ -n "$tn" ]; then
    echo "${tn^^}"
    return
  fi
  echo TENANT_1  # default
}
resolve_customer_name() {
  # used only if license activation is disabled
  if [ -n "$CUSTOMER_NAME" ]; then
    echo "$CUSTOMER_NAME"
  else
    echo CUSTOMER_1
  fi
}

build_multiple_params() {
  # build_multiple_params --email "admin@gmail.com admin2@gmail.com" -> --email admin@gmail.com --email admin2gmail.com
  local item counter=0
  for item in $2; do
    if [ -n "$3" ] && [ "$counter" -eq "$3" ]; then return; fi
    [ -z "$item" ] && continue
    printf "%s %s " "$1" "$item"
    ((counter++))
  done
}

initialize_system() {
  # creates:
  # - non-system admin users for Rule Engine & Modular Service
  # - license entity based on LM response
  # - customer based on LM response
  # - tenant within the customer which represents this AWS account
  # - entity that represents defect dojo installation
  local lm_response customer_name tenant_name modular_service_password rule_engine_password license_key dojo_token="" activation_id

  export PATH="$PATH:/home/$FIRST_USER/.local/bin"

  echo "Installing obfuscation manager"
  pip3 install --user --break-system-packages --upgrade "$SRE_RELEASES_PATH/$(get_latest_local_release)/${OBFUSCATOR_ARTIFACT_NAME}[xlsx]"
  echo "Installing modular-cli"
  MODULAR_CLI_ENTRY_POINT=$MODULAR_CLI_ENTRY_POINT pip3 install --user --break-system-packages --upgrade "$SRE_RELEASES_PATH/$(get_latest_local_release)/$MODULAR_CLI_ARTIFACT_NAME"

  echo "Logging in to modular-cli"
  syndicate setup --username admin --password "$(get_kubectl_secret modular-api-secret system-password)" --api_path "http://127.0.0.1:8085" --json
  syndicate login --json

  echo "Logging in to Rule engine using system user"
  syndicate re configure --api_link http://rule-engine:8000/caas --json
  syndicate re login --username system_user --password "$(get_kubectl_secret rule-engine-secret system-password)" --json

  echo "Logging in to Modular Service using system user"
  syndicate admin configure --api_link http://modular-service:8040/dev --json
  syndicate admin login --username system_user --password "$(get_kubectl_secret modular-service-secret system-password)" --json

  echo "Generating passwords for modular-service and rule-engine non-system users"
  modular_service_password="$(generate_password)"
  rule_engine_password="$(generate_password)"
  patch_kubectl_secret "$RULE_ENGINE_SECRET_NAME" "admin-password" "$rule_engine_password"
  patch_kubectl_secret "$MODULAR_SERVICE_SECRET_NAME" "admin-password" "$modular_service_password"

  if [ -z "$DO_NOT_ACTIVATE_LICENSE" ]; then
    customer_name="$(jq ".customer_name" -r <<<"$(get_kubectl_secret lm-data lm-response)")"
    lm_response="$(get_kubectl_secret lm-data lm-response)"
  else
    customer_name="$(resolve_customer_name)"
  fi
  # here i must create customer if it does not exit
  if ! syndicate admin customer describe --name "$customer_name" >/dev/null 2>&1; then
    echo "Creating customer $customer_name"
    syndicate admin customer add --name "$customer_name" --display_name "${CUSTOMER_DISPLAY_NAME:-$customer_name}" $(build_multiple_params --admin "$ADMIN_EMAILS") --json
  fi
  echo "Creating modular service policy, role and user"
  syndicate admin policy add --name admin_policy --permissions_admin --customer_id "$customer_name" --json
  syndicate admin role add --name admin_role --policies admin_policy --customer_id "$customer_name" --json
  syndicate admin users create --username "$MODULAR_SERVICE_USERNAME" --password "$modular_service_password" --role_name admin_role --customer_id "$customer_name" --json

  echo "Creating custodian customer users"
  syndicate re policy add --name admin_policy --permissions_admin --effect allow --tenant '*' --description "Full admin access policy for customer" --customer_id "$customer_name" --json
  syndicate re role add --name admin_role --policies admin_policy --description "Admin customer role" --customer_id "$customer_name" --json
  syndicate re users create --username "$RULE_ENGINE_USERNAME" --password "$rule_engine_password" --role_name admin_role --customer_id "$customer_name" --json

  echo "Setting LM related settings"
  syndicate re setting lm config add --host "$(get_kubectl_secret lm-data api-link)" --json
  if [ -z "$DO_NOT_ACTIVATE_LICENSE" ]; then
    syndicate re setting lm client add --key_id "$(jq ".private_key.key_id" -r <<<"$lm_response")" --algorithm "$(jq ".private_key.algorithm" -r <<<"$lm_response")" --private_key "$(jq ".private_key.value" -r <<<"$lm_response")" --b64encoded --json
  fi

  echo "Logging in as customer users"
  syndicate admin login --username "$MODULAR_SERVICE_USERNAME" --password "$modular_service_password" --json
  syndicate re login --username "$RULE_ENGINE_USERNAME" --password "$rule_engine_password" --json

  if [ -z "$DO_NOT_ACTIVATE_LICENSE" ]; then
    echo "Adding tenant license"
    license_key=$(syndicate re license add --tenant_license_key "$(jq ".tenant_license_key" -r <<<"$lm_response")" --json | jq ".items[0].license_key" -r)
    syndicate re license activate --license_key "$license_key" --all_tenants --json  # can be removed with new version of sre
  fi

  if [ -z "$DO_NOT_ACTIVATE_TENANT" ]; then
    tenant_name="$(resolve_tenant_name)"
    echo "Activating tenant $tenant_name for the current aws account"
    local err=0
    syndicate admin tenant create --name "$tenant_name" \
                                  --display_name "Tenant $(account_id)" \
                                  --cloud AWS \
                                  --account_id "$(account_id)" \
                                  $(build_multiple_params --primary_contacts "$TENANT_PRIMARY_CONTACTS") \
                                  $(build_multiple_params --secondary_concats "$TENANT_SECONDARY_CONTACTS") \
                                  $(build_multiple_params --tenant_manager_contacts "$TENANT_MANAGER_CONTACTS") \
                                  $(build_multiple_params --default_owner "$TENANT_OWNER_EMAIL" 1) \
                                  --json || err=1
    if [ "$err" -ne 0 ]; then
      warn "could not create tenant"
    else
      echo "Activating region for tenant"
      for r in $TENANT_AWS_REGIONS; do
        [ -z "$r" ] && continue
        echo "Activating $r for tenant"
        syndicate admin tenant regions activate --tenant_name "$tenant_name" --region_name "$r" --json > /dev/null || warn "could not activate region $r"
      done
    fi
  fi

  echo "Getting Defect dojo token"
  while [ -z "$dojo_token" ]; do
    sleep 2
    dojo_token=$(curl -X POST -H 'content-type: application/json' "http://127.0.0.1:80/api/v2/api-token-auth/" -d "{\"username\":\"admin\",\"password\":\"$(get_kubectl_secret "$DEFECTDOJO_SECRET_NAME" system-password)\"}" | jq ".token" -r || true)
  done

  echo "Activating dojo installation for rule engine"
  activation_id=$(syndicate re integrations dojo add --url http://defectdojo:8080/api/v2 --api_key "$dojo_token" --description "Global dojo installation" --json | jq ".items[0].id" -r)
  syndicate re integrations dojo activate --integration_id "$activation_id" --all_tenants --scan_type "Generic Findings Import" --send_after_job --json
}

cmd_init() {
  local opts init_system="" target_user="" public_ssh_key="" re_username="" re_password="" admin_username="" admin_password="" new_password
  opts="$(getopt -o "h" --long "help,system,user:,public-ssh-key:,re-username:,re-password:,admin-username:,admin-password:" -n "$PROGRAM" -- "$@")"
  eval set -- "$opts"
  while true; do
    case "$1" in
      '-h'|'--help') cmd_init_usage; exit 0 ;;
      '--system') init_system="true"; shift ;;
      '--user') target_user="$2"; shift 2 ;;
      '--public-ssh-key') public_ssh_key="$2"; shift 2 ;;
      '--re-username') re_username="$2"; shift 2 ;;
      '--re-password') re_password="$2"; shift 2 ;;
      '--admin-username') admin_username="$2"; shift 2 ;;
      '--admin-password') admin_password="$2"; shift 2 ;;
      '--') shift; break ;;
    esac
  done

  if [ -z "$init_system" ] && [ -z "$target_user" ]; then
    die "either --system or --user must be specified"
  fi

  if [ -n "$init_system" ]; then
    if [ -f "$SRE_LOCAL_PATH/.success" ]; then
      die "Rule Engine was already initialized. Cannot do that again"
    fi
    if [ "$FIRST_USER" != "$(whoami)" ]; then
      die "system configuration can be performed only by '$FIRST_USER' user"
    fi
    echo "Initializing Rule Engine for the first time"
    initialize_system
    echo "Done"
    return
  fi

  # target_user must exist here
  local _username=1 _password=1
  [ -n "$re_username" ] && _username=0
  [ -n "$re_password" ] && _password=0
  if [ "$(( _username ^ _password ))" -eq 1 ]; then
    die "--re-username and --re-password must be specified together"
  fi

  _username=1 _password=1
  [ -n "$admin_username" ] && _username=0
  [ -n "$admin_password" ] && _password=0
  if [ "$(( _username ^ _password ))" -eq 1 ]; then
    die "--admin-username and --admin-password must be specified together"
  fi

  echo "Initializing Rule Engine for user $target_user"
  if user_exists "$target_user"; then
    echo "User already exists"
  else
    echo "User does not exist. Creating..."
    sudo useradd --create-home --shell /bin/bash --user-group "$target_user" || die "could not create a user"
  fi

  if [ -n "$public_ssh_key" ]; then
    echo "Public SSH key was given. Adding this key to user's authorized_keys"
    sudo su - "$target_user" <<EOF
    mkdir -p .ssh
    chmod 700 .ssh
    echo "$public_ssh_key" >> .ssh/authorized_keys
    chmod 600 .ssh/authorized_keys
EOF
  fi
  echo "Installing CLIs for $target_user"
  sudo su - "$target_user" <<EOF >/dev/null
  pip3 install --user --break-system-packages "$SRE_RELEASES_PATH/$(get_latest_local_release)/${OBFUSCATOR_ARTIFACT_NAME}[xlsx]"
  MODULAR_CLI_ENTRY_POINT=$MODULAR_CLI_ENTRY_POINT pip3 install --user --break-system-packages "$SRE_RELEASES_PATH/$(get_latest_local_release)/$MODULAR_CLI_ARTIFACT_NAME"
EOF

  local err=0
  kubectl exec service/modular-api -- ./modular.py user describe --username "$target_user" &>/dev/null || err=1

  if [ "$err" -ne 0 ]; then
    echo "Creating new modular-api user"
    new_password="$(generate_password 20 -hex)"
    kubectl exec service/modular-api -- ./modular.py user add --username "$target_user" --group admin_group --password "$new_password"
    sudo su - "$target_user" <<EOF
    echo "Logging in to modular-cli"
    ~/.local/bin/syndicate setup --username "$target_user" --password "$new_password" --api_path "http://127.0.0.1:8085"
    ~/.local/bin/syndicate login
EOF
  else
    echo "Modular api user has been initialized before"
  fi


  if [ -n "$re_username" ]; then
    echo "Logging in to Rule Engine"
    sudo su - "$target_user" <<EOF
    ~/.local/bin/syndicate re configure --api_link http://rule-engine:8000/caas
    ~/.local/bin/syndicate re login --username "$re_username" --password "$re_password"
EOF
  fi

  if [ -n "$admin_username" ]; then
    echo "Logging in to Modular Service"
    sudo su - "$target_user" <<EOF
    ~/.local/bin/syndicate admin configure --api_link http://modular-service:8040/dev
    ~/.local/bin/syndicate admin login --username "$admin_username" --password "$admin_password"
EOF
  fi
  echo "Done"
}

pull_artifacts () {
  # todo is that efficient at all?
  local tag name url
  tag="$(jq -r '.tag_name' <<<"$1")"
  mkdir -p "$SRE_RELEASES_PATH/$tag"

  while IFS= read -r asset; do
    name="$(jq -r '.name' <<< "$asset")"
    url="$(jq -r '.url' <<< "$asset")"
    echo "Going to download $name for release $tag"
    if curl -fLs "${GITHUB_CURL_HEADERS[@]}" -o "/tmp/$name" -H "Accept: application/octet-stream" "$url"; then
      mv "/tmp/$name" "$SRE_RELEASES_PATH/$tag/$name"
    else
      rm -f "/tmp/$name"
      warn "could not download $asset from release $1"
    fi
  done < <(jq -c '.assets[]' <<<"$1")
}
get_release_type() {
  if [ "$(jq '.draft' <<<"$1")" = 'true'  ]; then
    echo 'draft'
  elif [ "$(jq '.prerelease' <<<"$1")" = 'true' ]; then
    echo 'prerelease'
  else
    echo 'release'
  fi
}

update_sre_init() {
  # assuming that the target version already exists locally
  local err=0
  sudo cp "$SRE_RELEASES_PATH/$1/$SRE_INIT_ARTIFACT_NAME" /usr/local/bin/sre-init || err=1
  if [ "$err" -eq 0 ]; then
    sudo chmod +x /usr/local/bin/sre-init
  else
    echo "Could not update sre-init"
  fi
}
warn_if_update_available() {
  # this function is designed to remind a user to update if the update is available
  local current_release release_data
  current_release="$(get_helm_release_version "$HELM_RELEASE_NAME")" || return 1
  if release_data="$(get_new_github_release "$current_release")"; then
    warn "new $(get_release_type "$release_data") $(jq -r '.tag_name' <<<"$release_data") is available. Use 'sre-init update'"
  fi
}
make_update_notification() {
  if [ ! -f "$UPDATE_NOTIFICATION_FILE" ]; then
    warn_if_update_available || return 1
    echo "$UPDATE_NOTIFICATION_PERIOD:$(( $(date +%s) / UPDATE_NOTIFICATION_PERIOD ))" > "$UPDATE_NOTIFICATION_FILE"
    return
  fi
  # file exists
  local period passed
  IFS=':' read -r period passed <"$UPDATE_NOTIFICATION_FILE"
  if [ "$(( $(date +%s) / period ))" -ne "$passed" ]; then
    warn_if_update_available || return 1
    echo "$UPDATE_NOTIFICATION_PERIOD:$(( $(date +%s) / UPDATE_NOTIFICATION_PERIOD ))" > "$UPDATE_NOTIFICATION_FILE"
    return
  fi
}

cmd_update_list() {
  local opts iter_params=()
  opts="$(getopt -o "h" --long "help,allow-prereleases" -n "$PROGRAM" -- "$@")"
  eval set -- "$opts"
  while true; do
    case "$1" in
      -h|--help) cmd_update_list_usage; exit 0 ;;
      --allow-prereleases) iter_params=(--prerelease --draft); shift ;;
      '--') shift; break ;;
    esac
  done

  local tag_name current_release
  current_release="$(get_helm_release_version "$HELM_RELEASE_NAME")"
  while IFS= read -r item; do
    tag_name=$(jq -r '.tag_name' <<< "$item")
    if [[ "$current_release" = "$tag_name" ]]; then
      jq -rj '"\(.tag_name)* \(.published_at) \(.html_url) \(.prerelease) \(.draft)"' <<<"$item" | colorize GREEN
    fi
    [[ ! "$current_release" < "$tag_name" ]] && break
    jq -rj '"\(.tag_name) \(.published_at) \(.html_url) \(.prerelease) \(.draft)\n"' <<<"$item"
  done < <(iter_github_releases "${iter_params[@]}") | column --table --table-columns RELEASE,DATE,URL,PRERELEASE,DRAFT
}

cmd_update() {
  local opts auto_yes=0 current_release release_data latest_tag backup_name="" iter_params=() check=0 same_version=0 do_backup=1 do_patch='true' helm_values
  opts="$(getopt -o "hy" --long "help,yes,check,no-backup,no-patch,allow-prereleases,same-version,backup-name:" -n "$PROGRAM" -- "$@")"
  eval set -- "$opts"
  while true; do
    case "$1" in
      '-h'|'--help') cmd_update_usage; exit 0 ;;
      '-y'|'--yes') auto_yes=1; shift ;;
      '--check') check=1; shift ;;
      '--no-backup') do_backup=0; shift ;;
      '--no-patch') do_patch='false'; shift ;;
      '--backup-name') backup_name="$2"; shift 2 ;;
      '--allow-prereleases') iter_params=(--prerelease --draft); shift ;;
      '--same-version') same_version=1; shift ;;
      '--') shift; break ;;
    esac
  done
  current_release="$(get_helm_release_version "$HELM_RELEASE_NAME")"
  if [ "$same_version" -eq 1 ]; then
    release_data="$(get_github_release_by_tag "$current_release")" || warn "could not get release by tag $current_release"
    latest_tag="$current_release"
  else
    if ! release_data="$(get_new_github_release "$current_release" "${iter_params[@]}")"; then
      echo "Up-to-date"
      exit 0
    fi
    latest_tag="$(jq -r '.tag_name' <<<"$release_data")"
  fi
  # by here release is definitely available
  if [ "$check" -eq 1 ]; then
    warn "new $(get_release_type "$release_data") $latest_tag is available. Use 'sre-init update'"
    exit 1
  fi
  echo "The current installed version is $current_release"
  echo "New github $(get_release_type "$release_data") $latest_tag is available"

  echo "Going to update to $latest_tag"
  [[ $auto_yes -eq 1 ]] || yesno "Do you want to update?"
  echo "Updating to $latest_tag"
  if [ "$do_backup" -eq 1 ]; then
    [ -z "$backup_name" ] && backup_name="$AUTO_BACKUP_PREFIX$(date +%s)"
    echo "Making backup $backup_name"
    cmd_backup_create --name "$backup_name" --volumes=minio,mongo,vault
  fi
  if [ -n "$release_data" ]; then
    echo "Pulling new artifacts"
    pull_artifacts "$release_data"
  fi
  echo "Verifying that necessary helm chart exists"
  helm repo update syndicate || die "helm repo update failed"
  helm search repo syndicate/rule-engine --version "$latest_tag" --fail-on-no-result >/dev/null 2>&1 || die "$latest_tag version $HELM_RELEASE_NAME chart not found. Cannot update"
  echo "Making helm upgrade. It should not take more than $(( HELM_UPGRADE_TIMEOUT / 60 )) minutes"
  helm_values="$(helm get values "$HELM_RELEASE_NAME" -o json)"  # preserve only user-set values
  if ! helm upgrade "$HELM_RELEASE_NAME" syndicate/rule-engine --timeout "${HELM_UPGRADE_TIMEOUT}s" --wait --wait-for-jobs --version "$latest_tag" --reset-values --values <(echo "$helm_values") --set=patch.enabled="$do_patch"; then
    warn "helm upgrade failed. Rolling back to the previous version..."
    helm rollback "$HELM_RELEASE_NAME" 0 --wait || die "Helm rollback failed... Contact the support team"
    if [ "$do_backup" -eq 1 ] && [ "$do_patch" = 'true' ]; then
      echo "Data patch could've been performed and backup was also created. Restoring backup $backup_name"
      cmd_backup_restore --name "$backup_name"
    fi
    exit 1
  else
    echo "helm upgrade was successful"
  fi

  if [ -f "$SRE_RELEASES_PATH/$latest_tag/$OBFUSCATOR_ARTIFACT_NAME" ]; then
    echo "Upgrading obfuscation manager"
    pip3 install --user --break-system-packages --upgrade "$SRE_RELEASES_PATH/$latest_tag/${OBFUSCATOR_ARTIFACT_NAME}[xlsx]" >/dev/null
  fi
  if [ -f "$SRE_RELEASES_PATH/$latest_tag/$MODULAR_CLI_ARTIFACT_NAME" ]; then
    echo "Upgrading modular CLI"
    MODULAR_CLI_ENTRY_POINT=$MODULAR_CLI_ENTRY_POINT pip3 install --user --break-system-packages --upgrade "$SRE_RELEASES_PATH/$latest_tag/${MODULAR_CLI_ARTIFACT_NAME}" >/dev/null
  fi
  if [ -f "$SRE_RELEASES_PATH/$latest_tag/$SRE_INIT_ARTIFACT_NAME" ]; then
    echo "Trying to update sre-init"
    update_sre_init "$latest_tag"
  fi
  echo "Done"
}

cmd_nginx() {
  case "$1" in
    -h|--help) shift; cmd_nginx_usage "$@" ;;
    enable) shift; cmd_nginx_enable "$@" ;;
    disable) shift; cmd_nginx_disable "$@" ;;
    ls) shift; cmd_nginx_list "$@" ;;
    '') cmd_nginx_list "$@" ;;
    *) die "$(cmd_unrecognized)" ;;
  esac
}

cmd_nginx_list() {
  # TODO can be rewritten
  local port filename rows="" enabled=":"
  for file in /etc/nginx/sites-enabled/*; do
    port="$(grep -oP "listen \K\d+" < "$file")"
    filename="${file##*/}"
    rows+="$filename Enabled $port\n"
    enabled+="$filename:"
  done
  for file in /etc/nginx/sites-available/*; do
    filename="${file##*/}"
    if [[ "$enabled" = *:$filename:* ]]; then
      continue
    fi
    port="$(grep -oP "listen \K\d+" < "$file")"
    rows+="$filename Disabled $port\n"
  done
  printf "%b" "$rows" | column --table --table-columns NAME,STATUS,PORT
}

cmd_nginx_enable() {
  printf "Not implemented yet. Create link from /etc/nginx/sites-available to /etc/nginx/sites-enabled manually. Expose existing k8s service manually\n"
  exit 1
}

cmd_nginx_disable() {
  printf "Not implemented yet\n"
  exit 1
}

scale_for_volume() {
  # accepts volume name as one parameter and number of replicas as the second one
  [ ! -v PV_TO_DEPLOYMENTS["$1"] ] && return
  local deploy
  while read -r -d ',' deploy; do
    kubectl scale deployment "$deploy" --replicas="$2"
  done <<<"${PV_TO_DEPLOYMENTS["$1"]},"  # comma added here to make sure read catches the last segment
}

make_backup() {
  # accepts k8s persistent volume name as first parameter and destination folder as second parameter.
  # perform scaling to 0 before this method
  local host_path
  host_path="$(kubectl get pv "$1" -o jsonpath="{.spec.hostPath.path}")"
  if [ -z "$host_path" ]; then
    warn "volume $1 does not have hostPath"
    return 1
  fi
  while true; do
    if minikube ssh "sudo tar -czf /tmp/$1.tar.gz -C $host_path ." >/dev/null 2>&1
    then
      break
    fi
    warn "error occurred making tar archive. Trying again in 1 sec"
    sleep 1
  done
  minikube cp "$HELM_RELEASE_NAME:/tmp/$1.tar.gz" "$2/"
  sha256sum "$2/$1.tar.gz" > "$2/$1.sha256"
}
restore_backup() {
  # accepts k8s persistent volume name as first parameter and folder with backup as second parameter
  # perform scaling to 0 before this method
  local host_path
  host_path="$(kubectl get pv "$1" -o jsonpath="{.spec.hostPath.path}")"
  if [ -z "$host_path" ]; then
    warn "volume $1 does not have hostPath"
    return 1
  fi
  if [ ! -f "$2/$1.tar.gz" ]; then
    warn "tar archive does not exist for $1"
    return 1
  fi
  if [ ! -f "$2/$1.sha256" ]; then
    warn "sha256 sum does not match for volume $1"
    return 1
  fi
  sha256sum "$2/$1.sha256" --check || return 1
  minikube cp "$2/$1.tar.gz" "$HELM_RELEASE_NAME:/tmp/$1.tar.gz"
  minikube ssh "sudo rm -rf $host_path; sudo mkdir -p $host_path ; sudo tar --same-owner --overwrite -xzf /tmp/$1.tar.gz -C $host_path"  # todo what if error here
}
cmd_backup() {
  case "$1" in
    -h|--help) shift; cmd_backup_usage "$@" ;;
    create) shift; cmd_backup_create "$@" ;;
    ls) shift; cmd_backup_list "$@" ;;
    rm) shift; cmd_backup_rm "$@";;
    restore) shift; cmd_backup_restore "$@" ;;
    '') cmd_backup_list "$@" ;;
    *) die "$(cmd_unrecognized)" ;;
  esac
}

resolve_backup_path() {
  local version
  if [ -n "$1" ]; then
    [ -n "$2" ] && warn "--version is ignored because --path is specified"
    echo "$1" # ignoring version if path is specified
  elif [ -n "$2" ]; then
    echo "$SRE_BACKUPS_PATH/$2"
  else
    version="$(get_helm_release_version "$HELM_RELEASE_NAME")" || die "cannot resolve current installation version. Use --version"
    echo "$SRE_BACKUPS_PATH/$version"
  fi
}

cmd_backup_list() {
  local opts version="" path="" pvs size
  opts="$(getopt -o "hv:p:" --long "help,version:,path:" -n "$PROGRAM" -- "$@")"
  eval set -- "$opts"
  while true; do
    case "$1" in
      -h|--help) cmd_backup_list_usage; exit 0 ;;
      -v|--version) version="$2"; shift 2 ;;
      -p|--path) path="$2"; shift 2 ;;
      '--') shift; break ;;
    esac
  done
  path="$(resolve_backup_path "$path" "$version")"
  if [ ! -d "$path" ] || [ -z "$(ls -A "$path")" ]; then
    echo -e "No backups found in $path\nTip: use sre-init backup ls --version \$previous to see backups for older installations" >&2
    exit 0
  fi
  find "$path/"* -maxdepth 0 -type d -print0 | xargs -0 stat --format "%W %n" | sort -r | while IFS=' ' read -r ts fp; do
    pvs=$(find "$fp" -name '*.tar.gz' -type f -exec basename --suffix='.tar.gz' '{}' \; | sort | tr '\n' ',' | sed 's/,$//')
    size="$(du -hsc "$fp"/*.tar.gz 2>/dev/null | grep total | cut -f1 || true)"
    printf "%s|%s|%s|%s\n" "$(basename "$fp")" "$(date --date="@$ts")" "${size:-0}" "$pvs"
  done | column --table -s "|" --table-columns NAME,DATE,SIZE,PVs
}

cmd_backup_rm() {
  local opts version="" path="" name="" auto_yes=0
  opts="$(getopt -o "n:hyv:p:" --long "name:,help,yes,version:,path:" -n "$PROGRAM" -- "$@")"
  eval set -- "$opts"
  while true; do
    case "$1" in
      -h|--help) cmd_backup_rm_usage; exit 0 ;;
      -v|--version) version="$2"; shift 2 ;;
      -p|--path) path="$2"; shift 2 ;;
      -n|--name) name="$2"; shift 2 ;;
      -y|--yes) auto_yes=1; shift ;;
      '--') shift; break ;;
    esac
  done
  [ -z "$name" ] && die "--name is required"
  path="$(resolve_backup_path "$path" "$version")"
  if [ ! -d "$path/$name" ]; then
    echo "All traces of '$name' (from $path) are removed"
    exit 0
  fi
  [[ $auto_yes -eq 1 ]] || yesno "Do you really want to remove backup?"
  rm -rf "${path:?}/$name"
  echo "All traces of '$name' (from $path) are removed"
}

cmd_backup_create() {
  local opts path="" name="" volumes="" vol items=()
  opts="$(getopt -o "n:hp:" --long "name:,help,path:,volumes:" -n "$PROGRAM" -- "$@")"
  eval set -- "$opts"
  while true; do
    case "$1" in
      -h|--help) cmd_backup_create_usage; exit 0 ;;
      -p|--path) path="$2"; shift 2 ;;
      -n|--name) name="$2"; shift 2 ;;
      --volumes) volumes="$2"; shift 2 ;;
      '--') shift; break ;;
    esac
  done
  [ -z "$name" ] && die "--name is required"
  if [ -z "$path" ]; then
    path="$SRE_BACKUPS_PATH/$(get_helm_release_version "$HELM_RELEASE_NAME")" || die "cannot resolve current version"
  fi
  [ -d "$path/$name" ] && die "'$name' already exists"
  if [ -z "$volumes" ]; then
    for vol in $(kubectl get pv -o=jsonpath="{.items[*].metadata.name}" 2>/dev/null); do
      items+=("$vol")
    done
  else
    while read -r -d ',' vol; do
      [ -z "$vol" ] && continue
      if ! kubectl get pv "$vol" >/dev/null 2>&1; then
        warn "'$vol' volume does not exist"
        continue
      fi
      items+=("$vol")
    done <<<"$volumes,"
  fi
  [ "${#items[@]}" -eq 0 ] && die "no volumes to make backup"
  mkdir -p "$path/$name"
  for vol in "${items[@]}"; do
    echo "Making backup for volume $vol"
    scale_for_volume "$vol" 0
    make_backup "$vol" "$path/$name" || warn "could not make backup"
    scale_for_volume "$vol" 1
  done
}
cmd_backup_restore() {
  local opts path="" name="" volumes="" version="" force=0 current_release vol
  opts="$(getopt -o "n:hp:v:f" --long "name:,help,path:,version:,volumes:,force" -n "$PROGRAM" -- "$@")"
  eval set -- "$opts"
  while true; do
    case "$1" in
      -h|--help) cmd_backup_restore_usage; exit 0 ;;
      -p|--path) path="$2"; shift 2 ;;
      -v|--version) version="$2"; shift 2 ;;
      -n|--name) name="$2"; shift 2 ;;
      -f|--force) force=1; shift ;;
      --volumes) volumes="$2"; shift 2 ;;
      '--') shift; break ;;
    esac
  done
  [ -z "$name" ] && die "--name is required"
  current_release="$(get_helm_release_version "$HELM_RELEASE_NAME")" || die "cannot resolve current version"
  [ "$force" -eq 0 ] && [ -n "$version" ] && [ "$version" != "$current_release" ] && die "current release $current_release does not match to the backup version $version. Specify --force if you really want to restore backup"
  path="$(resolve_backup_path "$path" "$version")"
  [ ! -d "$path/$name" ] && die "backup '$name' (from $path) not found"

  declare -a items
  if [ -z "$volumes" ]; then
    while IFS= read -r -d ''; do
      items+=("$(basename --suffix='.tar.gz' "$REPLY")")
    done < <(find "$path/$name" -name '*.tar.gz' -type f -print0)
  else
    IFS=',' read -ra items <<< "$volumes"
  fi

  for vol in "${items[@]}"; do
    [ -z "$vol" ] && continue
    if ! kubectl get pv "$vol" >/dev/null 2>&1; then
      warn "'$vol' volume does not exist"
      continue
    fi
    echo "Restoring volume '$vol'"
    scale_for_volume "$vol" 0
    restore_backup "$vol" "$path/$name" || warn "could not restore backup"
    scale_for_volume "$vol" 1
  done
}

cmd_secrets() {
  if [ -z "$1" ]; then
    printf "%s\n" "${!SECRETS_MAPPING[@]}"
    return
  fi
  [ ! -v SECRETS_MAPPING["$1"] ] && die "there is no secret $1"
  IFS=',' read -ra values <<< "${SECRETS_MAPPING[$1]}"
  get_kubectl_secret "${values[0]}" "${values[1]}" || die "cannot reach secret. Probably, you don't have access"
}

cmd_health() {
  local opts
  opts="$(getopt -o "h" --long "help" -n "$PROGRAM" -- "$@")"
  eval set -- "$opts"
  while true; do
    case "$1" in
      -h|--help) cmd_health_usage; exit 0 ;;
      '--') shift; break ;;
    esac
  done
  declare -A checks  # order is priority
  checks["1:/usr/local/sre/.success"]="test -f /usr/local/sre/.success"
  checks["2:Rule Engine helm release"]="helm get metadata $HELM_RELEASE_NAME"
  checks["3:Syndicate entrypoint"]="syndicate version"
  checks["4:Rule Engine health check"]="syndicate re health_check"
  checks["5:Obfuscation manager entrypoint"]="sreobf --help"
  checks["6:Defect Dojo helm release"]="helm get metadata defectdojo"

  # TODO allow to show error message for each check
  while IFS= read -r key; do
    IFS=":" read -r order name <<<"$key"
    if ${checks["$name"]} >/dev/null 2>&1; then
      printf "%s|%s|ok" "$order" "$name" | colorize GREEN
    else
      printf "%s|%s|failed" "$order" "$name" | colorize RED
      exit 1
    fi
    printf "\n"
  done < <(printf "%s\n" "${!checks[@]}" | sort) | column --table -s "|" --table-columns "№,CHECK,STATUS"
}

# Start
VERSION="1.0.0"
PROGRAM="${0##*/}"
COMMAND="$1"

# Some global variables that can be provided from outside
AUTO_BACKUP_PREFIX="${AUTO_BACKUP_PREFIX:-autobackup-}"
SRE_LOCAL_PATH="${SRE_LOCAL_PATH:-/usr/local/sre}"
SRE_RELEASES_PATH="${SRE_RELEASES_PATH:-$SRE_LOCAL_PATH/releases}"
SRE_BACKUPS_PATH="${SRE_BACKUPS_PATH:-$SRE_LOCAL_PATH/backups}"
GITHUB_REPO="${GITHUB_REPO:-epam/syndicate-rule-engine}"
HELM_RELEASE_NAME="${HELM_RELEASE_NAME:-rule-engine}"
HELM_UPGRADE_TIMEOUT="${HELM_UPGRADE_TIMEOUT:-120}"
DO_NOT_ACTIVATE_LICENSE="${DO_NOT_ACTIVATE_LICENSE:-}"
DO_NOT_ACTIVATE_TENANT="${DO_NOT_ACTIVATE_TENANT:-}"

# for --system configuration
MODULAR_SERVICE_USERNAME="${MODULAR_SERVICE_USERNAME:-admin}"
RULE_ENGINE_USERNAME="${RULE_ENGINE_USERNAME:-admin}"
TENANT_AWS_REGIONS="${TENANT_AWS_REGIONS:-us-east-1 us-east-2 us-west-1 us-west-2 af-south-1 ap-east-1 ap-south-2 ap-southeast-3 ap-southeast-4 ap-south-1 ap-northeast-3 ap-northeast-2 ap-southeast-1 ap-southeast-2 ap-northeast-1 ca-central-1 ca-west-1 eu-central-1 eu-west-1 eu-west-2 eu-south-1 eu-west-3 eu-south-2 eu-north-1 eu-central-2 il-central-1 me-south-1 me-central-1 sa-east-1 us-gov-east-1 us-gov-west-1}"
FIRST_USER="${FIRST_USER:-$(getent passwd 1000 | cut -d : -f 1)}"
#CUSTOMER_NAME=  # resolved dynamically, see corresponding method
#CUSTOMER_DISPLAY_NAME=  # can be used
#TENANT_NAME=  # resolved dynamically, see corresponding method
#ADMIN_EMAILS=
#TENANT_PRIMARY_CONTACTS=
#TENANT_SECONDARY_CONTACTS=
#TENANT_MANAGER_CONTACTS=
#TENANT_OWNER_EMAIL=
# specify emails split by " "


# All variables below are constants and should not be changed
RULE_ENGINE_SECRET_NAME=rule-engine-secret
MODULAR_API_SECRET_NAME=modular-api-secret
MODULAR_SERVICE_SECRET_NAME=modular-service-secret
DEFECTDOJO_SECRET_NAME=defectdojo-secret

MODULAR_CLI_ENTRY_POINT=syndicate

declare -A SECRETS_MAPPING
SECRETS_MAPPING["dojo-system-password"]="$DEFECTDOJO_SECRET_NAME,system-password"
SECRETS_MAPPING["modular-service-system-password"]="$MODULAR_SERVICE_SECRET_NAME,system-password"
SECRETS_MAPPING["modular-service-admin-password"]="$MODULAR_SERVICE_SECRET_NAME,admin-password"
SECRETS_MAPPING["rule-engine-system-password"]="$RULE_ENGINE_SECRET_NAME,system-password"
SECRETS_MAPPING["rule-engine-admin-password"]="$RULE_ENGINE_SECRET_NAME,admin-password"
SECRETS_MAPPING["modular-api-system-password"]="$MODULAR_API_SECRET_NAME,system-password"

# FOR backups to scale up/down
declare -A PV_TO_DEPLOYMENTS
PV_TO_DEPLOYMENTS["minio"]="minio"
PV_TO_DEPLOYMENTS["vault"]="vault"
PV_TO_DEPLOYMENTS["mongo"]="mongo"
PV_TO_DEPLOYMENTS["defectdojo-cache"]="defectdojo-redis"
PV_TO_DEPLOYMENTS["defectdojo-data"]="defectdojo-postgres"
PV_TO_DEPLOYMENTS["defectdojo-media"]="defectdojo-nginx,defectdojo-uwsgi,defectdojo-celeryworker"


GITHUB_CURL_HEADERS=('-H' 'X-GitHub-Api-Version: 2022-11-28')
if [ -n "$GITHUB_TOKEN" ]; then
  GITHUB_CURL_HEADERS+=('-H' "Authorization: Bearer $GITHUB_TOKEN")
fi


MODULAR_CLI_ARTIFACT_NAME=modular_cli.tar.gz
OBFUSCATOR_ARTIFACT_NAME=sre_obfuscator.tar.gz
SRE_INIT_ARTIFACT_NAME=sre-init.sh

# in seconds
UPDATE_NOTIFICATION_PERIOD="${UPDATE_NOTIFICATION_PERIOD:-3600}"
UPDATE_NOTIFICATION_FILE="$SRE_LOCAL_PATH/.update-notification"

make_update_notification || true

case "$1" in
  backup) shift; cmd_backup "$@" ;;
  help|-h|--help) shift; cmd_usage "$@" ;;
  version|--version) shift; cmd_version "$@" ;;
  update) shift; cmd_update "$@" ;;
  list) shift; cmd_update_list "$@" ;;
  init) shift; cmd_init "$@" ;;
  nginx) shift; cmd_nginx "$@" ;;
  secrets) shift; cmd_secrets "$@" ;;
  health) shift; cmd_health "$@" ;;
  --system|--user) cmd_init "$@" ;;  # redirect to init as default one
  '') cmd_usage ;;
  *) die "$(cmd_unrecognized)" ;;
esac
exit 0