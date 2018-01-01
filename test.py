
#!/usr/bin/env python
# encoding=utf8


from request import RequestService
from extract import ExtractService
from seeds import SeedsService

SeedsService.start()


seed = SeedsService.get_template('content', 'page', 'lianjia')

seed.url = 'https://cd.lianjia.com/ershoufang/106100750158.html'

res = RequestService.request(seed)

cont = ExtractService.extract(seed, res[2])
