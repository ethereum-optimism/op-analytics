import re
import subprocess
import shlex
from urllib.parse import urlparse


MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\((?P<destination>[^)]+)\)")


def main():
    # Run the command to find markdown links and capture the output
    found_links = subprocess.check_output(
        shlex.split(r"""find . -name "*.md" -exec grep -HoP '\[([^\]]+)\]\(([^)]+)\)' {} \;"""),
        text=True,
    )

    for link in found_links.split("\n"):
        if not link:
            continue

        link_file, markdown_link = link.split(":", maxsplit=1)

        destination_match = MARKDOWN_LINK_RE.match(markdown_link)
        if not destination_match:
            print(f"Invalid link: {markdown_link}")
            continue
        destination = destination_match.group("destination")

        if destination.startswith("/"):
            # The destination is an absolute path
            check_absolute_path(destination)

        elif destination.startswith("."):
            # The destination is a relative path
            check_relative_path(link_file, destination)

        elif destination.startswith(("http://", "https://")):
            # The destination is a URL
            parsed_url = urlparse(destination)
            if parsed_url.scheme and parsed_url.netloc:
                check_url(destination)
        else:
            continue


def check_absolute_path(destination: str):
    print("ABS: ", destination)
    return


def check_relative_path(link_file: str, destination: str):
    print("REL: ", link_file, destination)
    return


def check_url(destination: str):
    print("URL: ", destination)
    return


if __name__ == "__main__":
    main()
