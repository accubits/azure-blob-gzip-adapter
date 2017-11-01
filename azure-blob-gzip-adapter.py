from azure.storage.blob import BlockBlobService
import zlib
import io,csv

acc_name = '' #Account Name
acc_key = '' #Account Key
container = '' #Container
blob = "" #Name of blob
number_of_fields = 1 #Number of fields in the gzip csv

block_blob_service = BlockBlobService(account_name=acc_name, account_key=acc_key)


props = block_blob_service.get_blob_properties(container, blob)
blob_size = int(props.properties.content_length)
chunk_size = 1024
index = 0
output = io.BytesIO()
carry_fwd = ''
obj = zlib.decompressobj(16 + zlib.MAX_WBITS)


def worker(data):
    # Add your code here
    print data


while index < blob_size:
    block_blob_service.get_blob_to_stream(container, blob, output, start_range=index,end_range=index + chunk_size - 1,max_connections=2)
    if output is None:
        continue
    output.seek(index)
    data = output.read()
    length = len(data)
    print length
    index += length
    if length > 0:
        try:
            deCompressed = obj.decompress(data)
        except Exception, e:
            print e.message
            continue
        if (carry_fwd != ''):
            deCompressed = carry_fwd+deCompressed
        if deCompressed:
            csvData = list(csv.reader(deCompressed.split('\n'), delimiter=","))
            for each in csvData:
                if len(each) == number_of_fields:
                    carry_fwd = ''
                    worker(each)
                else:
                    carry_fwd = ",".join(each)
        if length < chunk_size:
            break
    else:
        break