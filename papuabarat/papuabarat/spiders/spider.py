import scrapy
import json
import os
from datetime import datetime
import requests
import s3fs

class papuaBaratSpider(scrapy.Spider):
    name = 'spider'
    start_urls = ['https://papuabaratprov.go.id/web/home/header-pages?slug=potensi']

    def save_json(self, data, filename):
        with open(filename, 'w') as f:
            json.dump(data, f)

    def download_image(self, img_url, img_path):
        response = requests.get(img_url)
        with open(img_path, 'wb') as img_file:
            img_file.write(response.content)

    def upload_to_s3(self, local_path, raw_path):
        client_kwargs = {
            'key': '',
            'secret': '',
            'endpoint_url': '',
            'anon': 
        }

        s3 = s3fs.core.S3FileSystem(**client_kwargs)
        s3.upload(rpath=raw_path, lpath=local_path)

        if s3.exists(raw_path):
            self.logger.info('File upload successfully')
        else:
            self.logger.info('File upload failed')

    def parse(self, response):
        #link img and desc -> response.css('div.thumbinner a img::attr(src), div.thumbinner div::text').getall()
        title = response.xpath('//*[@id="Potensi"]/text()').get()
        potensi_content = response.xpath('//*[@id="textContentSection"]/div/div/div/div/div/div/div/div/div/p[position() <=3] //text()').getall()
        for i in range(len(potensi_content)):
            potensi_content[i] = potensi_content[i].replace('\u00a0', ' ')
        suku_bangsa = response.xpath('//*[@id="textContentSection"]/div/div/div/div/div/div/div/div/div/p[position() >= 4 and position() <= 6 ] //text()').getall()
        for i in range(len(suku_bangsa)):
            suku_bangsa[i] = suku_bangsa[i].replace('\u00a0', ' ')
        kebudayaan = response.xpath('//*[@id="textContentSection"]/div/div/div/div/div/div/div/div/div/p[position() >= 7] //text()').getall()
        for i in range(len(kebudayaan)):
            kebudayaan[i] = kebudayaan[i].replace('\u00a0', ' ')

        descriptions = response.css('div.thumbinner div.thumbcaption::text').getall()
        image_urls = response.css('div.thumbinner a img::attr(src)').getall()
        images_data = []
        for img_url, desc in zip(image_urls, descriptions):
            img_name = img_url.split('/')[-1]

            
            file_path = 'image'
            dir_path = os.path.join('data_raw',file_path)
            os.makedirs(dir_path, exist_ok=True)
            img_path = os.path.join(dir_path, img_name)


            for desc in descriptions:
                desc = desc.replace('\u00a0', ' ').replace('\r', '').replace('\n', '')

            self.download_image(img_url, img_path)

            local_path_img = f'D:/Visual Studio Code/Work/papuabaratprov.go.id/papuabarat/data_raw/image/{img_name}'
            s3_path_img = f's3://ai-pipeline-statistics/data/data_raw/papuabaratprov.go.id/potensi Papua/image/{img_name}'
            
            self.upload_to_s3(local_path_img, s3_path_img)


            images_data.append({
                'image_filename': img_name,
                'description': desc,
                's3_path_image': s3_path_img
            })

        data_table = []
        rows = response.css('table.wikitable tbody tr')
        for row in rows[1:]:
            no = row.xpath('.//td[1]/text()').get()
            suku = row.xpath('.//td[2]/text()').get()
            jumlah_2010 = row.xpath('.//td[3]/text()').get()
            persentase = row.xpath('.//td[4]/text()').get()

            data_table.append({
                'No': no,
                'Suku': suku,
                'Jumlah 2010': jumlah_2010,
                '%': persentase,
            })

            for i in range(len(data_table)):
                data_table[i]['Suku'] = data_table[i]['Suku'].replace('\u00a0', ' ')

        dir_path = 'data_raw'
        dir_raw = os.path.join(dir_path)
        os.makedirs(dir_raw, exist_ok=True)
        filename = 'potensi.json'

        s3_path = f's3://ai-pipeline-statistics/data/data_raw/papuabaratprov.go.id/potensi Papua/json/{filename}'
        local_path = f'D:/Visual Studio Code/Work/papuabaratprov.go.id/papuabarat/data_raw/{filename}'
        data={
            'link' : response.url,
            'domain' : 'papuabaratprov.go.id',
            'tag' : [
                'papuabaratprov.go.id',
                title
            ],
            'crawling_time' : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'crawling_time_epoch' : int(datetime.now().timestamp()),
            'path_data_raw' : s3_path,
            'path_data_clean' : None,
            'potensi_content' : potensi_content,
            'suku_bangsa' : [{
                'content' : suku_bangsa,
                'data_table' : data_table,
        }],
            'kebudayaan_content' : kebudayaan,
            'image_data' : images_data
            }
        self.save_json(data, os.path.join(dir_raw, filename))
        self.upload_to_s3(local_path, s3_path)


