# patch_add_row_ids.py
import re
import html
from pathlib import Path

INPUT = Path("Monitor_page.txt")
OUTPUT = Path("Monitor_page_patched.html")

def sanitize_for_id(s: str) -> str:
    # convert to ASCII-equivalent, lowercase, replace non-alnum with hyphen
    s = html.unescape(s)
    s = re.sub(r'\s+', ' ', s).strip()
    s = s.strip()
    s = re.sub(r'[^0-9A-Za-z]+', '-', s)
    s = re.sub(r'-+', '-', s)
    s = s.strip('-')
    if not s:
        return "unknown"
    # keep original capitalization-ish? we'll return as TitleCase-ish
    return s

text = INPUT.read_text(encoding="utf-8")

# We'll find table rows and the second <td> content (the checklist).
# Approach: iterate through matches of <tr>...</tr> and for each, find second <td> innerText.
out = []
pos = 0

tr_pattern = re.compile(r'(<tr\b[^>]*>)(.*?)(</tr>)', flags=re.DOTALL | re.IGNORECASE)
td_pattern = re.compile(r'<td\b[^>]*>(.*?)</td>', flags=re.DOTALL | re.IGNORECASE)

def has_id_attr(tag_open: str) -> bool:
    return re.search(r'\bid\s*=', tag_open, flags=re.IGNORECASE) is not None

for m in tr_pattern.finditer(text):
    start, end = m.span()
    tag_open, inner, tag_close = m.group(1), m.group(2), m.group(3)

    # only attempt to add id if not present already
    if has_id_attr(tag_open):
        out.append(text[pos:start])
        out.append(text[start:end])
        pos = end
        continue

    # find td matches
    tds = list(td_pattern.finditer(inner))
    if len(tds) >= 2:
        # second td contains checklist
        td2_html = tds[1].group(1)
        # strip tags inside and decode entities
        td2_text = re.sub(r'<[^>]+>', '', td2_html).strip()
        sanitized = sanitize_for_id(td2_text)
        key_id = f'row-{sanitized}'
        # insert id into tag_open
        new_tag_open = tag_open.rstrip()[:-1] + f' id="{key_id}">' if tag_open.strip().endswith('>') else tag_open + f' id="{key_id}">'
        new_tr = new_tag_open + inner + tag_close
        out.append(text[pos:start])
        out.append(new_tr)
        pos = end
    else:
        out.append(text[pos:start])
        out.append(text[start:end])
        pos = end

out.append(text[pos:])
OUTPUT.write_text(''.join(out), encoding="utf-8")
print(f"Patched HTML written to: {OUTPUT.resolve()}")
