from camunda.client.engine_client import EngineClient


def main():
    client = EngineClient()
    resp_json = client.correlate_message("REJECT_APPLICATION",
                                         tenant_id="2fb74cfc-5f4b-4666-80af-3aa47e954721",
                                         business_key="78c5ea34-12bc-11eb-94e0-acde48001122")
    print(resp_json)


if __name__ == '__main__':
    main()
