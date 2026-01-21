"""
Extended Bot - ASCII Logo with QR Code
"""

import pyfiglet
import qrcode
import re

# Telegram канал
channel_link = "https://t.me/sviche_crypto"

def print_logo():
    """Печать ASCII логотипа с QR-кодом"""

    # 1. ASCII логотип "EXTENDED"
    ascii_lines = pyfiglet.figlet_format("EXTENDED", font="slant").splitlines()
    ascii_lines = [line for line in ascii_lines if line.strip()]
    logo_width = max(len(l) for l in ascii_lines)

    # 2. Панель с информацией о Telegram
    title = "\x1b[1;35mTelegram Channel\x1b[0m"
    title_visible_len = len(re.sub(r'\x1b\[[0-9;]*m', '', title))
    title_pad_left = (logo_width - 2 - title_visible_len) // 2
    title_pad_right = logo_width - 2 - title_visible_len - title_pad_left

    panel_texts = [
        '',
        "\x1b[1;33mПодписывайся — больше софтов\x1b[0m",
        "\x1b[1;33mи инфы по Extended!         \x1b[0m",
        '',
        f"\x1b[1;37m{channel_link}\x1b[0m"
    ]

    panel_lines = []
    panel_lines.append('┌' + '─' * (logo_width - 2) + '┐')
    panel_lines.append('│' + ' ' * title_pad_left + title + ' ' * title_pad_right + '│')

    for t in panel_texts:
        visible_len = len(re.sub(r'\x1b\[[0-9;]*m', '', t))
        panel_lines.append('│' + t + ' ' * (logo_width - 2 - visible_len) + '│')

    panel_lines.append('└' + '─' * (logo_width - 2) + '┘')

    # 3. QR-код для Telegram
    qr = qrcode.QRCode(version=1, box_size=1, border=0)
    qr.add_data(channel_link)
    qr.make(fit=True)
    qr_matrix = qr.get_matrix()

    qr_lines = []
    for y in range(0, len(qr_matrix), 2):
        top = qr_matrix[y]
        bottom = qr_matrix[y+1] if y+1 < len(qr_matrix) else [0]*len(top)
        line = ''
        for t, b in zip(top, bottom):
            if t and b:
                line += '▓'
            elif t and not b:
                line += '▀'
            elif not t and b:
                line += '▄'
            else:
                line += ' '
        qr_lines.append(line)

    # Отступ между элементами
    pad = '         '  # 9 пробелов

    # 1. Верхняя часть: ASCII логотип + QR-код
    for i in range(len(ascii_lines)):
        l = ascii_lines[i]
        r = qr_lines[i] if i < len(qr_lines) else ''
        print(f"\x1b[36m{l}\x1b[0m{pad}\x1b[2m{r}\x1b[0m")

    # 2. Средняя часть: Telegram-панель + QR-код
    for i in range(len(panel_lines)):
        l = panel_lines[i]
        r = qr_lines[len(ascii_lines) + i] if (len(ascii_lines) + i) < len(qr_lines) else ''
        print(f"{l}{pad}\x1b[2m{r}\x1b[0m")

    # 3. Нижняя часть: только QR-код (если он выше)
    for i in range(len(ascii_lines) + len(panel_lines), len(qr_lines)):
        l = ' ' * logo_width
        r = qr_lines[i]
        print(f"{l}{pad}\x1b[2m{r}\x1b[0m")

    # Отступ перед основной программой
    print('   ')


if __name__ == "__main__":
    print_logo()
