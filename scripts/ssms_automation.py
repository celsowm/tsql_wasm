import argparse
import os
import re
import subprocess
import sys
import time
from typing import Iterable, Optional

from pywinauto import Application, Desktop
from pywinauto.keyboard import send_keys


EXIT_OK = 0
EXIT_AUTOMATION_ERROR = 1
EXIT_PREREQ_ERROR = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Automate SSMS startup, connection, and Object Explorer expansion."
    )
    parser.add_argument("--server", default="tcp:127.0.0.1,1433")
    parser.add_argument("--user", default="sa")
    parser.add_argument("--password", default="Iridium12345!")
    parser.add_argument("--connect-timeout", type=int, default=120)
    parser.add_argument("--launch-timeout", type=int, default=120)
    parser.add_argument("--post-connect-timeout", type=int, default=120)
    parser.add_argument("--ssms-path", default=None)
    return parser.parse_args()


def get_ssms_path(override_path: Optional[str]) -> Optional[str]:
    if override_path:
        return override_path if os.path.exists(override_path) else None

    possible_paths = [
        r"C:\Program Files\Microsoft SQL Server Management Studio 21\Release\Common7\IDE\SSMS.exe",
        r"C:\Program Files (x86)\Microsoft SQL Server Management Studio 19\Common7\IDE\Ssms.exe",
        r"C:\Program Files (x86)\Microsoft SQL Server Management Studio 18\Common7\IDE\Ssms.exe",
    ]

    for path in possible_paths:
        if os.path.exists(path):
            return path

    try:
        out = subprocess.check_output(
            'powershell -Command "Get-ChildItem -Path \'C:\\Program Files*\' -Filter Ssms.exe -Recurse -ErrorAction SilentlyContinue | Select-Object -ExpandProperty FullName"',
            shell=True,
        ).decode(errors="ignore")
        lines = [line.strip() for line in out.splitlines() if line.strip()]
        if lines:
            return lines[0]
    except Exception:
        pass

    return None


def iter_windows() -> Iterable:
    return Desktop(backend="uia").windows()


def as_spec(win):
    try:
        return Desktop(backend="uia").window(handle=win.handle)
    except Exception:
        return None


def safe_text(value: str) -> str:
    enc = getattr(sys.stdout, "encoding", None) or "utf-8"
    return value.encode(enc, errors="replace").decode(enc, errors="replace")


def log(msg: str) -> None:
    print(safe_text(msg))


def window_title(win) -> str:
    try:
        return (win.window_text() or "").strip()
    except Exception:
        return ""


def find_main_window() -> Optional:
    for win in iter_windows():
        title = window_title(win)
        if "SQL Server Management Studio" in title:
            return win
    return None


def looks_like_connect_dialog(win) -> bool:
    title = window_title(win).lower()
    if (
        "connect to server" in title
        or "conectar ao servidor" in title
        or "conectar-se" in title
        or ("versão prévia" in title and "conectar" in title)
    ):
        return True
    if not title:
        return False

    cls = ""
    try:
        cls = (win.class_name() or "").lower()
    except Exception:
        pass

    if cls == "#32770":
        spec = as_spec(win)
        if spec is None:
            return False
        try:
            has_server = spec.child_window(auto_id="ComboBox_1", control_type="ComboBox").exists(timeout=0)
            has_auth = spec.child_window(auto_id="AuthenticationComboBox", control_type="ComboBox").exists(timeout=0)
            if has_server or has_auth:
                return True
        except Exception:
            return False

    # SSMS 21 Preview can expose connect UI as a main "Window" with no classic dialog title.
    spec = as_spec(win)
    if spec is None:
        return False
    try:
        has_ok = spec.child_window(auto_id="OkButton", control_type="Button").exists(timeout=0)
        has_advanced = spec.child_window(auto_id="AdvancedPropertiesButton", control_type="Button").exists(timeout=0)
        if has_ok and has_advanced:
            return True
    except Exception:
        pass
    return False


def find_connect_dialog() -> Optional:
    candidates = [win for win in iter_windows() if looks_like_connect_dialog(win)]
    if not candidates:
        return None

    def score(win) -> int:
        try:
            edits = len(win.descendants(control_type="Edit"))
            combos = len(win.descendants(control_type="ComboBox"))
            buttons = len(win.descendants(control_type="Button"))
            texts = [((t.window_text() or "").lower()) for t in win.descendants(control_type="Text")[:200]]
            has_connect_labels = any(
                marker in txt
                for marker in (
                    "server name",
                    "nome do servidor",
                    "user name",
                    "authentication",
                    "aut_hentication",
                    "password",
                )
                for txt in texts
            )
            return edits + combos + buttons + (1000 if has_connect_labels else 0)
        except Exception:
            return 0

    return max(candidates, key=score)


def dump_windows() -> None:
    log("--- Current Visible Windows (UIA) ---")
    for win in iter_windows():
        title = window_title(win)
        cls = ""
        try:
            cls = win.class_name()
        except Exception:
            cls = ""
        log(f"TITLE: {title} | CLASS: {cls}")
    log("-------------------------------------")


def set_control_text(ctrl, value: str) -> bool:
    try:
        ctrl.set_focus()
    except Exception:
        pass

    for setter in ("set_edit_text", "type_keys"):
        try:
            if setter == "set_edit_text":
                ctrl.set_edit_text(value)
            else:
                send_keys("^a{BACKSPACE}")
                ctrl.type_keys(value, with_spaces=True, set_foreground=True)
            return True
        except Exception:
            continue

    try:
        send_keys("^a{BACKSPACE}")
        send_keys(value, with_spaces=True)
        return True
    except Exception:
        return False


def trigger_connect_dialog_shortcuts() -> None:
    # Try multiple ways because SSMS versions/locales differ.
    main_win = find_main_window()
    if main_win is not None:
        try:
            main_win.set_focus()
            main_win.type_keys("^+c", set_foreground=True)
        except Exception:
            send_keys("^+c")
    else:
        send_keys("^+c")
    time.sleep(0.5)
    send_keys("{F8}")  # Object Explorer toggle as fallback
    time.sleep(0.5)


def wait_for_connect_dialog(timeout_seconds: int) -> Optional:
    start = time.time()
    while time.time() - start < timeout_seconds:
        dialog = find_connect_dialog()
        if dialog:
            return dialog
        elapsed = int(time.time() - start)
        if elapsed in (10, 25, 45, 70):
            log(f"Connect dialog still not found ({elapsed}s), trying shortcuts...")
            trigger_connect_dialog_shortcuts()
        time.sleep(1)
    return None


def try_select_sql_auth(dialog) -> bool:
    dialog_spec = as_spec(dialog)
    if dialog_spec is None:
        return False

    auth_combo = None
    auth_candidates = [
        {"auto_id": "AuthenticationComboBox", "control_type": "ComboBox"},
        {"title_re": ".*Authentication.*", "control_type": "ComboBox"},
        {"title_re": ".*Autentica.*", "control_type": "ComboBox"},
    ]

    for candidate in auth_candidates:
        try:
            ctrl = dialog_spec.child_window(**candidate)
            if ctrl.exists(timeout=0):
                auth_combo = ctrl.wrapper_object()
                break
        except Exception:
            continue

    if auth_combo is None:
        log("Authentication combo not found; will continue with fallback.")
        return False

    labels = [
        "SQL Server Authentication",
        "Autenticação do SQL Server",
        "SQL Server",
    ]
    for label in labels:
        try:
            auth_combo.select(label)
            return True
        except Exception:
            continue

    try:
        auth_combo.set_focus()
        send_keys("%{DOWN}")
        time.sleep(0.3)
        send_keys("s{ENTER}")
        return True
    except Exception:
        return False


def fill_connect_dialog(dialog, server: str, user: str, password: str) -> None:
    dialog_spec = as_spec(dialog)
    dialog.set_focus()
    time.sleep(0.5)

    server_set = False
    user_set = False
    password_set = False

    # SSMS 21 Preview: map right-pane edits by vertical position.
    try:
        preview_edits = []
        for _ in range(60):
            current_dialog = find_connect_dialog() or dialog
            dlg_rect = current_dialog.rectangle()
            x_min = dlg_rect.left + int(dlg_rect.width() * 0.40)
            y_min = dlg_rect.top + int(dlg_rect.height() * 0.40)
            y_max = dlg_rect.top + int(dlg_rect.height() * 0.60)

            preview_edits = []
            for edit in current_dialog.descendants(control_type="Edit"):
                try:
                    if not edit.is_visible():
                        continue
                    rect = edit.rectangle()
                    if rect.left < x_min:
                        continue
                    if y_min <= rect.top <= y_max:
                        preview_edits.append(edit.wrapper_object())
                except Exception:
                    continue
            preview_edits.sort(key=lambda e: e.rectangle().top)
            if len(preview_edits) >= 3:
                break
            time.sleep(0.5)

        log(f"Preview edit candidates: {len(preview_edits)}")
        if len(preview_edits) >= 3:
            def fill_one(ctrl, value: str) -> bool:
                try:
                    ctrl.set_focus()
                    time.sleep(0.1)
                    send_keys("^a{BACKSPACE}")
                    time.sleep(0.1)
                    send_keys(value, with_spaces=True)
                    time.sleep(0.1)
                    return True
                except Exception:
                    return False

            server_set = fill_one(preview_edits[0], server)
            user_set = fill_one(preview_edits[1], user)
            password_set = fill_one(preview_edits[2], password)
            log(f"Preview fill result server={server_set} user={user_set} password={password_set}")
    except Exception:
        pass

    server_candidates = [
        {"auto_id": "ComboBox_1", "control_type": "ComboBox"},
        {"title_re": ".*Server name.*", "control_type": "ComboBox"},
        {"title_re": ".*Nome do servidor.*", "control_type": "ComboBox"},
    ]

    for candidate in server_candidates:
        if dialog_spec is None:
            break
        try:
            ctrl = dialog_spec.child_window(**candidate)
            if ctrl.exists(timeout=0):
                server_set = set_control_text(ctrl.wrapper_object(), server)
                if server_set:
                    break
        except Exception:
            continue

    if not server_set:
        log("Server field control not found; using TAB fallback.")
        send_keys("{TAB}{TAB}")
        send_keys("^a{BACKSPACE}")
        send_keys(server, with_spaces=True)

    if try_select_sql_auth(dialog):
        log("Authentication set to SQL Server Authentication.")
    else:
        log("Could not confirm authentication selection; proceeding.")

    user_candidates = [
        {"auto_id": "UserNameComboBox", "control_type": "ComboBox"},
        {"title_re": ".*Login.*", "control_type": "Edit"},
        {"title_re": ".*Usuário.*", "control_type": "Edit"},
    ]
    for candidate in user_candidates:
        if dialog_spec is None:
            break
        try:
            ctrl = dialog_spec.child_window(**candidate)
            if ctrl.exists(timeout=0):
                user_set = set_control_text(ctrl.wrapper_object(), user)
                if user_set:
                    break
        except Exception:
            continue

    if not user_set:
        log("User field not found directly; trying keyboard fallback.")
        send_keys("{TAB}")
        send_keys("^a{BACKSPACE}")
        send_keys(user, with_spaces=True)

    pass_candidates = [
        {"auto_id": "PasswordPasswordBox", "control_type": "Edit"},
        {"title_re": ".*Password.*", "control_type": "Edit"},
        {"title_re": ".*Senha.*", "control_type": "Edit"},
    ]
    for candidate in pass_candidates:
        if dialog_spec is None:
            break
        try:
            ctrl = dialog_spec.child_window(**candidate)
            if ctrl.exists(timeout=0):
                password_set = set_control_text(ctrl.wrapper_object(), password)
                if password_set:
                    break
        except Exception:
            continue

    if not password_set:
        log("Password field not found directly; trying keyboard fallback.")
        send_keys("{TAB}")
        send_keys("^a{BACKSPACE}")
        send_keys(password, with_spaces=True)

    # SSMS Preview fallback: no stable auto_ids for Server/User/Password edits.
    if not server_set or not user_set or not password_set:
        try:
            edits = []
            for edit in dialog.descendants(control_type="Edit"):
                try:
                    if edit.is_visible():
                        edits.append(edit.wrapper_object())
                except Exception:
                    continue

            if len(edits) >= 3:
                if not server_set:
                    server_set = set_control_text(edits[0], server)
                if not user_set:
                    user_set = set_control_text(edits[1], user)
                if not password_set:
                    password_set = set_control_text(edits[2], password)
        except Exception:
            pass


def click_connect(dialog) -> None:
    dialog_spec = as_spec(dialog)
    try:
        if dialog_spec is not None:
            btn = dialog_spec.child_window(auto_id="OkButton", control_type="Button")
            if btn.exists(timeout=0):
                wrapper = btn.wrapper_object()
                try:
                    wrapper.invoke()
                except Exception:
                    wrapper.click_input()
                return
    except Exception:
        pass

    button_patterns = [
        {"title": "Connect", "control_type": "Button"},
        {"title": "Conectar", "control_type": "Button"},
        {"title_re": ".*Connect.*", "control_type": "Button"},
        {"title_re": ".*Conectar.*", "control_type": "Button"},
    ]

    for pattern in button_patterns:
        try:
            if dialog_spec is not None:
                btn = dialog_spec.child_window(**pattern)
                if btn.exists(timeout=0):
                    wrapper = btn.wrapper_object()
                    try:
                        wrapper.invoke()
                    except Exception:
                        wrapper.click_input()
                    return
        except Exception:
            continue

    send_keys("{ENTER}")


def ensure_history_tab(dialog) -> None:
    dialog_spec = as_spec(dialog)
    if dialog_spec is None:
        return

    patterns = [
        {"title": "History", "control_type": "TabItem"},
        {"title_re": "Hist.*", "control_type": "TabItem"},
        {"title": "History", "control_type": "Text"},
        {"title_re": "Hist.*", "control_type": "Text"},
    ]
    for pattern in patterns:
        try:
            ctrl = dialog_spec.child_window(**pattern)
            if ctrl.exists(timeout=0):
                ctrl.wrapper_object().click_input()
                time.sleep(0.3)
                return
        except Exception:
            continue


def try_connect_from_recent(dialog, server: str, user: str) -> bool:
    dialog_spec = as_spec(dialog)
    if dialog_spec is None:
        return False

    ensure_history_tab(dialog)
    server_no_prefix = server.lower().replace("tcp:", "").strip()
    host_only = server_no_prefix.split(",")[0].strip()
    user_only = user.strip().lower()

    try:
        candidates = dialog.descendants(control_type="ListItem")
    except Exception:
        return False

    for item in candidates:
        try:
            txt = (item.window_text() or "").lower()
            if not txt:
                continue
            host_match = host_only in txt
            user_match = f"({user_only})" in txt or user_only in txt
            if host_match and user_match:
                wrapper = item.wrapper_object()
                try:
                    wrapper.double_click_input()
                except Exception:
                    wrapper.click_input()
                time.sleep(0.5)
                click_connect(dialog)
                log(f"Connected using recent entry: {item.window_text()}")
                return True
        except Exception:
            continue

    return False


def handle_certificate_popup(timeout_seconds: int = 20) -> None:
    start = time.time()
    cert_terms = ("certificate", "certificado", "security", "segur")

    while time.time() - start < timeout_seconds:
        acted = False
        for win in iter_windows():
            title = window_title(win).lower()
            if not title:
                continue
            if not any(term in title for term in cert_terms):
                continue

            win_spec = as_spec(win)
            for text in ("Yes", "Sim", "OK", "Connect", "Conectar"):
                try:
                    if win_spec is not None:
                        btn = win_spec.child_window(title=text, control_type="Button")
                        if btn.exists(timeout=0):
                            btn.wrapper_object().click_input()
                            log(f"Handled certificate/security popup: {window_title(win)}")
                            acted = True
                            break
                except Exception:
                    continue

            if acted:
                break
        if not acted:
            time.sleep(0.5)


def ensure_object_explorer_visible(main_win) -> None:
    main_spec = as_spec(main_win)
    try:
        if main_spec is not None:
            tree = main_spec.child_window(control_type="Tree")
            if tree.exists(timeout=0):
                return
    except Exception:
        pass

    main_win.set_focus()
    send_keys("{F8}")
    time.sleep(1)
    send_keys("%v")
    time.sleep(0.3)
    send_keys("e")
    time.sleep(1)


def get_object_explorer_tree(main_win) -> Optional:
    main_spec = as_spec(main_win)
    if main_spec is None:
        return None
    for _ in range(20):
        try:
            tree = main_spec.child_window(control_type="Tree")
            if tree.exists(timeout=1):
                return tree.wrapper_object()
        except Exception:
            pass
        time.sleep(1)
    return None


def connection_established(main_win, server: str) -> bool:
    tree = get_object_explorer_tree(main_win)
    if tree is None:
        return False

    server_tokens = [part for part in re.split(r"[,:]", server) if part]
    try:
        for item in tree.children():
            text = (item.window_text() or "").lower()
            if not text:
                continue
            if any(token.lower() in text for token in server_tokens):
                return True
    except Exception:
        return False
    return False


def expand_tree_to_tables(main_win) -> bool:
    tree = get_object_explorer_tree(main_win)
    if tree is None:
        log("Object Explorer tree not found.")
        return False

    try:
        roots = tree.children()
        if not roots:
            log("Object Explorer tree has no root nodes.")
            return False

        root = roots[0]
        root.expand()
        time.sleep(2)

        databases_node = None
        for child in root.children():
            text = (child.window_text() or "").lower()
            if "databases" in text or "bancos de dados" in text:
                databases_node = child
                break

        if databases_node is None:
            log("Databases node not found.")
            return False

        databases_node.expand()
        time.sleep(2)

        db_nodes = databases_node.children()
        if not db_nodes:
            log("No database nodes found under Databases.")
            return False

        first_db = db_nodes[0]
        first_db.expand()
        time.sleep(2)

        tables_node = None
        for child in first_db.children():
            text = (child.window_text() or "").lower()
            if "tables" in text or "tabelas" in text:
                tables_node = child
                break

        if tables_node is None:
            log("Tables node not found.")
            return False

        for attempt in range(1, 4):
            tables_node.expand()
            time.sleep(2)
            table_children = tables_node.children()
            if table_children:
                print("Tables expanded successfully:")
                for item in table_children[:20]:
                    log(f"  - {item.window_text()}")
                return True
            log(f"No table children visible on attempt {attempt}; refreshing.")
            tables_node.set_focus()
            send_keys("{F5}")
            time.sleep(4)

    except Exception as exc:
        log(f"Error navigating Object Explorer tree: {exc}")
        return False

    return False


def launch_ssms(ssms_path: str, launch_timeout: int):
    log(f"Launching SSMS: {ssms_path}")
    try:
        app = Application(backend="uia").start(ssms_path)
    except Exception as exc:
        log(f"pywinauto start() failed: {exc}. Falling back to subprocess.")
        subprocess.Popen([ssms_path])
        app = Application(backend="uia").connect(path=ssms_path, timeout=launch_timeout)

    start = time.time()
    while time.time() - start < launch_timeout:
        if find_main_window() is not None or find_connect_dialog() is not None:
            return app
        time.sleep(1)

    raise TimeoutError("SSMS launch timeout: no main window or connect dialog detected.")


def automate(args: argparse.Namespace) -> int:
    ssms_path = get_ssms_path(args.ssms_path)
    if not ssms_path:
        log("SSMS not found. Provide --ssms-path or install SSMS.")
        return EXIT_PREREQ_ERROR

    try:
        launch_ssms(ssms_path, args.launch_timeout)
        warmup_start = time.time()
        while time.time() - warmup_start < 15:
            main_win = find_main_window()
            if main_win is not None:
                ensure_object_explorer_visible(main_win)
                if connection_established(main_win, args.server):
                    log("Connection already established in Object Explorer. Skipping connect dialog.")
                    if not expand_tree_to_tables(main_win):
                        log("Connected, but failed to expand tree to Tables.")
                        return EXIT_AUTOMATION_ERROR
                    log("Automation completed successfully.")
                    return EXIT_OK
            time.sleep(1)

        dialog = wait_for_connect_dialog(args.connect_timeout)
        if dialog is None:
            log("Connect to Server dialog not found.")
            dump_windows()
            return EXIT_AUTOMATION_ERROR

        log(f"Found connect dialog: {window_title(dialog)}")
        used_recent = try_connect_from_recent(dialog, args.server, args.user)
        if not used_recent:
            fill_connect_dialog(dialog, args.server, args.user, args.password)
            click_connect(dialog)
        handle_certificate_popup()
        log("Connect action sent.")

        start = time.time()
        connected = False
        while time.time() - start < args.post_connect_timeout:
            main_win = find_main_window()
            if main_win is None:
                time.sleep(1)
                continue
            ensure_object_explorer_visible(main_win)
            if connection_established(main_win, args.server):
                connected = True
                break
            time.sleep(1)

        if not connected:
            log("Connection was not confirmed in Object Explorer.")
            return EXIT_AUTOMATION_ERROR

        main_win = find_main_window()
        if main_win is None:
            log("Main SSMS window not found after connection.")
            return EXIT_AUTOMATION_ERROR

        ensure_object_explorer_visible(main_win)
        if not expand_tree_to_tables(main_win):
            log("Connected, but failed to expand tree to Tables.")
            return EXIT_AUTOMATION_ERROR

        log("Automation completed successfully.")
        return EXIT_OK

    except Exception as exc:
        log(f"Fatal automation error: {exc}")
        return EXIT_AUTOMATION_ERROR


if __name__ == "__main__":
    sys.exit(automate(parse_args()))

