import re

def parse_slurm_host_names(data: str):
    """
    from the input data parse the hot name list
    the input data is like x2,x-gpu05

    We simply change the "," into space
    """
    if data.find(",") >0:
        # replace the "," with space
        return data.replace(",", " ")
    else:
        return data

def parse_tres_data_from_json(data):
    """
    this function is used to parse the json data fields

    the example format is like gres/gpu:rtxa6000:1 or gres/gpu:3

    we will return the number of GPU cards captured from the data field
    """
    data_tmp = data.strip().lower()

    # do we have any data
    gpus = 0
    ncpus = 0
    nnodes = 0
    mem = 0
    if "," in data_tmp:
        data_fields = data_tmp.split(',')
        for x in data_fields:

            # the data format is like gres/gpu=3 or gres/gpus:xx=3 (for gpu), or cpu=20, mem=40 etc.
            if "=" in x:
                value = x.split("=")[-1]
            else:
                print(data)
                raise RuntimeError("The input data field does not have equal sign =, failed for parsing!\n")

            # data fields
            # for memory, usually it's like the data 320G etc.
            if x.find("gres")>=0:
                gpus = int(value)
            elif x.find("cpu")>=0:
                ncpus = int(value)
            elif x.find("node")>=0:
                nnodes = int(value)
            elif x.find("mem")>=0:
                s = value.lower()
                if s.find("g")>=0:
                    mem = int(re.sub(r'\D', '', value))
                elif s.find("m") >= 0:
                    mem = int(int(re.sub(r'\D', '', value))/1024)
                elif s.find("t") >= 0:
                    mem = int(re.sub(r'\D', '', value))*1024
                else:
                    print(data)
                    raise RuntimeError("The input data field for memory does not have any unit "
                                       "sign like kb, gb or tb, failed for parsing!\n")
    else:
        print(data)
        raise RuntimeError("The input data field can not be parsed, it should have multiple data fields like ncpus=1,mem=10G\n")

    # double check the data
    # at least the memory/cpus/nnodes should be defined
    if mem == 0 or nnodes == 0 or ncpus ==0:
        print(data)
        raise RuntimeError("The input data field should have at least memory/ncpus/n_nodes defined, "
                           "now at least one of them is missing in the input data\n")

    # return
    return nnodes,ncpus,mem,gpus
