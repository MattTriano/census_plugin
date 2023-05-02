import re
import requests
from typing import Dict

import pandas as pd

class CensusAPICatalog:
    def __init__(self):
        self.set_data_catalog_json()
        self.set_dataset_metadata()

    def set_data_catalog_json(self) -> pd.DataFrame:
        url = "https://api.census.gov/data.json"
        resp = requests.get(url)

        if resp.status_code == 200:
            data_catalog_json = resp.json()
            self.data_catalog_json = data_catalog_json
        else:
            raise Exception(f"Failed to get a valid response; status_code: {resp.status_code}")

    def set_dataset_metadata(self) -> None:
        if "dataset" in self.data_catalog_json.keys():
            datasets = self.data_catalog_json["dataset"]
            print(f"Elements in Census data catalog datasets attr: {len(datasets)} ")
            df_list = []
            df_shape_list = []
            for dataset in datasets:
                df = pd.json_normalize(dataset)
                df_list.append(df)
                df_shape_list.append(df.shape)
            full_df = pd.concat(df_list)
            full_df = full_df.reset_index(drop=True)
            full_df["modified"] = pd.to_datetime(full_df["modified"])

            col_order = [
                "title", "identifier", "modified", "temporal", "bureauCode", "programCode",
                "description", "keyword", "spatial", "c_vintage", "c_dataset", "c_geographyLink",
                "c_variablesLink", "c_tagsLink", "c_examplesLink", "c_groupsLink", "c_sorts_url",
                "c_documentationLink", "c_isAggregate", "c_isCube", "c_isAvailable", "c_isTimeseries",
                "c_isMicrodata", "@type",  "accessLevel", "distribution", "license", "references",
                "contactPoint.fn", "contactPoint.hasEmail", "publisher.@type", "publisher.name",
                "publisher.subOrganizationOf.@type", "publisher.subOrganizationOf.name",
                "publisher.subOrganizationOf.subOrganizationOf.@type",
                "publisher.subOrganizationOf.subOrganizationOf.name"
            ]
            full_df = full_df[col_order].copy()
            distribution_df = pd.json_normalize(full_df["distribution"].str[0])
            distribution_df.columns = [f"distribution_{col}" for col in distribution_df.columns]
            full_df = pd.merge(left=full_df, right=distribution_df, how="left", left_index=True, right_index=True)
            full_df = full_df.sort_values(by="modified", ascending=False, ignore_index=True)
            self.dataset_metadata = full_df
        else:
            raise Exception(f"field 'dataset' not found in data_catalog response")


class CensusDatasetSource:
    def __init__(self, base_api_call: str, media_type: str = "json"):
        self.base_api_call = base_api_call
        self.media_type = media_type

    def get_detail_url(self, detail_type: str) -> str:
        return f"{self.base_api_call}/{detail_type}.{self.media_type}"

    @property
    def variables_url(self):
        return self.get_detail_url(detail_type="variables")

    @property
    def examples_url(self):
        return self.get_detail_url(detail_type="examples")

    @property
    def sorts_url(self):
        return self.get_detail_url(detail_type="sorts")

    @property
    def geographies_url(self):
        return self.get_detail_url(detail_type="geography")

    @property
    def tags_url(self):
        return self.get_detail_url(detail_type="tags")

    @property
    def groups_url(self):
        return self.get_detail_url(detail_type="groups")

    def get_url_response(self, url: str) -> Dict:
        api_call = re.sub("\.html$", ".json", url)
        resp = requests.get(api_call)
        if resp.status_code == 200:
            resp_json = resp.json()
            return resp_json
        else:
            print(f"Failed to get a valid response; status code: {resp.status_code}")
            return None

    @property
    def variables_df(self) -> None:
        variables_resp_json = self.get_url_response(self.variables_url)
        variables_df = pd.DataFrame(variables_resp_json["variables"]).T
        variables_df.index.name = "variable"
        variables_df = variables_df.reset_index()
        variables_df["predicateOnly"] = variables_df["predicateOnly"].fillna(False)
        return variables_df