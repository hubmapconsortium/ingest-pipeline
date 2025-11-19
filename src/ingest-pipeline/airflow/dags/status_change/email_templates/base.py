from status_change.status_utils import Project


class EmailTemplate:
    footer = [
        "",
        "This email address is not monitored. Please email ingest@hubmapconsortium.org with any questions about your data submission.",
    ]

    def __init__(
        self,
        data: dict,
        project: Project,
        entity_id: str,
        log_dir_path: str,
        error_counts: str,
        error_dict: dict,
    ):
        self.data = data
        self.project = project
        self.entity_id = entity_id
        self.error_counts = error_counts
        self.error_dict = error_dict
        self.uuid = self.data.get("uuid")
        self.entity_type = self.data.get("entity_type", "")
        self.status = self.data.get("status", "")
        self.log_directory_path = log_dir_path

    def format(self) -> tuple[str, str]:
        raise NotImplementedError

    def stringify(self, message: list) -> str:
        lines = []
        for line in message:
            lines.append(line + "<br>" if not line.endswith(("</li>", "</ul>", "<ul>")) else line)
        return "".join(lines)

    @property
    def html_formatted_error_list(self) -> list:
        if self.error_dict:
            return self.recursive_format_dict(self.error_dict, html=True)
        return []

    def recursive_format_dict(self, errors, html: bool = False) -> list:
        """ """
        error_list = []
        if html:
            error_list.append("<ul>")
        if isinstance(errors, dict):
            if html:
                list_of_error_lists = [
                    [f"<li>{k.strip()}:</li>", *self.recursive_format_dict(v, html=html)]
                    for k, v in errors.items()
                ]
            else:
                list_of_error_lists = [
                    [f"{k.strip()}:", *self.recursive_format_dict(v, html=html)]
                    for k, v in errors.items()
                ]

            for e_list in list_of_error_lists:
                error_list.extend(e_list)
        elif isinstance(errors, list):
            list_of_errors = [v.strip() for v in errors]
            if html:
                list_of_errors = [f"<li>{v}</li>" for v in list_of_errors]
            for e in list_of_errors:
                error_list.append(e)
        else:
            error = errors.strip()
            if html:
                error = f"<li>{error}</li>"
            error_list.append(error)
        if html:
            error_list.append("</ul>")
        return error_list
