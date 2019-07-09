from flask import Flask, request
import requests
from pymongo import MongoClient
from flask_restplus import Resource, Api, fields, reqparse
import datetime
import re

"""Write with python 3.5.1"""
app = Flask(__name__)
api = Api(app, default="Indicators",  # Default namespace
          title="World Bank Economic Indicators",  # Documentation Title
          description="Data Service for World Bank Economic Indicators")

"""indicator json"""
indicator_field = api.model('indicator', {
    'id': fields.String,
})


"""option parameter q"""
parser = reqparse.RequestParser()
parser.add_argument('q')



@api.route('/indicators')
class GetIndicators(Resource):
    @api.response(201, 'Record Created Successfully')
    @api.response(200, 'Record already exists')
    @api.response(404, 'Validation Error')
    @api.doc(description="Add a new indicator")
    @api.expect(indicator_field)
    def post(self):
        id = request.json['id']
        if id in all_ids:
            if db.ass2.find_one({"indicator": id}) is None:
                url = "http://api.worldbank.org/v2/countries/all/indicators/"+id+"?date=2012:2017&format=json&per_page=2000"
                data = requests.get(url)
                if data.json()[1] is None:
                    return {
                    "message": "Collection = {} does not have any information!".format(id)
                        },200
                id_json = data.json()[1]
                collection = {}
                collection['collection_id'] = id_json[0]['indicator']['id']
                collection['indicator'] = id_json[0]['indicator']['id']
                collection['indicator_value'] = id_json[0]['indicator']['value']
                collection['time'] = str(datetime.datetime.now())
                entries = []
                for obj in id_json:
                    entries.append({'country':obj['country']['value'],"date":obj['date'],"value":obj['value']})
                collection['entries'] = entries
                db.ass2.insert_one(collection)
                return {
                    "location":"/indicators/"+ collection['collection_id'],
                    "collection_id": collection['collection_id'],
                    "creation_time": collection['time'],
                    "indicator": collection['collection_id']
                },201
            else:
                exit_indicator = db.ass2.find_one({"indicator": id})
                return {
                           "message" : id + " already exists!",
                           "location": "/indicators/" + exit_indicator['collection_id'],
                           "collection_id": exit_indicator['collection_id'],
                           "creation_time": exit_indicator['time'],
                           "indicator": exit_indicator['collection_id']
                       }, 200

        else:
            return {"message": "indicator id {} does not exist".format(id)}, 404

    @api.doc(description="Get indicator list")
    def get(self):
        if db.ass2.count()==0:
            return {"message": "The database is empty!"}, 200
        else:
            result = []
            all_items = db.ass2.find()
            for item in all_items:
                result.append({
                    "location" : "/indicators/" + item['collection_id'],
                    "collection_id": item['collection_id'],
                    "creation_time": item['time'],
                    "indicator":  item['indicator']

                })
            return result,200

@api.route('/indicators/<string:id>')
class Indicators(Resource):
    @api.response(404, 'indicator id was not found')
    @api.doc(description="Delete a indicator by its ID")
    def delete(self,id):
        if id in all_ids:
            obj = db.ass2.find_one({"collection_id": id})
            if obj is None:
                return {
                    "message": "Collection = {} does not exist from the database!".format(id)
                },404
            else:
                db.ass2.delete_one({'collection_id': id})
                return {
                    "message": "Collection = {} is removed from the database!".format(id)
                },200
        else:
            return {
                    "message" :"{} does not exist!".format(id)
            },404

    @api.response(404, 'Indicator id was not found')
    @api.response(200, 'Successful')
    @api.doc(description="Get indicator by its ID")
    def get(self,id):
        if id in all_ids:
            obj = db.ass2.find_one({"collection_id": id})
            if obj is None:
                return {
                    "message": "Collection = {} does not exist from the database!".format(id)
                },404
            else:
                return {
                    "collection_id": obj['collection_id'],
                    "indicator": obj['indicator'],
                    "indicator_value": obj['indicator_value'],
                    "creation_time": obj['time'],
                    "entries": obj['entries']
                },200
        else:
            return {
                       "message": "{} does not exist!".format(id)
                   }, 404


@api.route('/indicators/<string:id>/<int:year>/<string:country>')
class IndicatorsCountry(Resource):
    @api.response(400, 'Validation Error')
    @api.response(404, 'Indicator id was not found')
    @api.response(200, 'Successful')
    @api.doc(description="Retrieve economic indicator value for given country and a year")
    def get(self,id,year,country):
        if id in all_ids:
            if year<2012 or year>2017:
                return {
                           "message": "{} is not a valid year! It should be between 2012 and 2017".format(year)
                       }, 400
            obj = db.ass2.find_one({"collection_id": id})
            if obj is None:
                return {
                           "message": "Collection = {} does not exist from the database!".format(id)
                       }, 404
            else:
                for entry in obj['entries']:
                    if entry['country']==country and int(entry['date'])==year:
                        return {
                            "collection_id": obj['collection_id'],
                            "indicator": obj['indicator'],
                            "country": country,
                            "year": year,
                            "value": entry['value']
                        },200
                return {
                       "message": "Economic indicator value of {} in {} does not exist from collection {}!".format(country ,year, id)
                    }, 404
        else:
            return {
                       "message": "{} does not exist!".format(id)
                   }, 404


@api.route('/indicators/<string:id>/<int:year>')
class indicatorlist(Resource):
    @api.expect(parser)
    @api.response(400, 'Validation Error')
    @api.response(404, 'Indicator id was not found')
    @api.response(200, 'Successful')
    @api.doc(description="Retrieve top/bottom economic indicator values for a given year")
    def get(self,id,year):
        query = parser.parse_args()['q']
        top_pattern = re.compile(r'\btop[1-9][0-9]{,2}\b')
        bottom_pattern = re.compile(r'\bbottom[1-9][0-9]{,2}\b')
        istop = False
        if query:
            if top_pattern.match(query):
                query = int(query.replace('top',""))
                if query<1 or query>100:
                    return {
                               "message": "The query number should be between 1 and 100"
                           }, 400
                istop = True
            elif bottom_pattern.match(query):
                query = int(query.replace('bottom',""))
                if query<1 or query>100:
                    return {
                               "message": "The query number should be between 1 and 100"
                           }, 400
            else:
                return {
                               "message": "The query is not valid"
                           },400
        if id in all_ids:
            if year<2012 or year>2017:
                return {
                           "message": "{} is not a valid year! It should be between 2012 and 2017".format(year)
                       }, 400
            obj = db.ass2.find_one({"collection_id": id})
            if obj is None:
                return {
                           "message": "Collection = {} does not exist from the database!".format(id)
                       }, 404
            else:
                result = []
                for entry in obj['entries']:
                    if int(entry['date'])==year:
                        result.append(entry)
                result.sort(key=getValue, reverse=True)
                if query:
                    if istop:
                        return {
                           "indicator": obj['indicator'],
                           "indicator_value": obj['indicator_value'],
                           "entries": result[:query]
                               }, 200
                    else:
                        result = result[::-1]
                        return {
                                   "indicator": obj['indicator'],
                                   "indicator_value": obj['indicator_value'],
                                   "entries": result[:query]
                               }, 200
                else:
                    return {
                        "indicator" : obj['indicator'],
                        "indicator_value": obj['indicator_value'],
                        "entries":result
                    },200
        else:
            return {
                       "message": "{} does not exist!".format(id)
                   }, 404


def getValue(obj):
    if obj['value'] is None:
        return -1
    return int(obj['value'])


if __name__ == '__main__':
    uri = "mongodb://ass2:9321ass2@ds145752.mlab.com:45752/comp9321ass2"
    client = MongoClient(uri,connectTimeoutMS=30000)
    db = client.get_database('comp9321ass2')
    """I stored all indicator ids in my database, it will take some time to load all ids"""
    all_ids = db.all_indicators.find({})[0]
    app.run()

