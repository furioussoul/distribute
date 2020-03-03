from PIL import Image
import json

def deal_line(line):
    line = line.replace("/home/yangjunqi/pytorch-landmark/PFLD-pytorch/data", "/Users/szj/Downloads")
    img_path = line.split(" ")[0]
    line_split = line.split(" ")
    axises = line_split[:len(line_split) - 9]
    img = Image.open(img_path)
    label_file_path = img_path.replace("png","txt")

    coordinates = []

    i = 1
    xy = {
        "classification":1
    }

    label_file = open(label_file_path,'w')

    l = len(axises)

    for axis in axises:
        label_file.write(axis)
        if l != i:
          label_file.write(" ")
        else:
            print(123)
        i+=1

    # if i % 2 != 0:
        #     xy['axisX'] = str(int(float(axis) * img.width))
        # else:
        #     xy['axisY'] = str(int(float(axis) * img.height))
        #     coordinates.append(xy)
        #     xy = {
        #         "classification":1
        #     }
        # i += 1


    # label_file.writelines(json.dumps([{
    #     "classification":{},
    #     "coordinates":coordinates
    # }]))







path = "/Users/szj/Downloads/test_data/list.txt"
file = open(path)

for line in file:
    deal_line(line)

file.close()
