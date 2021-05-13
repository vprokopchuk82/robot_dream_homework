from hdfs import InsecureClient
import json

def main():
    client=get_hdfs_client()
    data = [{"name": "Anne", "salary": 10000}, {"name": "Victor", "salary": 9500}]
    with client.write('/test/sample_file.json', encoding='utf-8') as json_file_in_hdfs:
        json.dump(data, json_file_in_hdfs)




def get_hdfs_client():
    global hdfs_client
    return InsecureClient(f'http://localhost:50070/', 'root')

if __name__ == '__main__':
    main()
