import re
import copy
import sys
import json
import pymongo

from lingua_pddl.parser import Parser

class MongoKB:
  def __init__(self):
      self.client = None
      self.db = None
      self.objects = None
      self.types = None

      self._collections = {}

      self._handlers = {}

  def connect(self):
      self.client = pymongo.MongoClient()
      self.db = self.client.lingua

      self.objects = self.db.objects
      self.types = self.db.types

      self._collections = {
        'types': self.types, 
        'objects': self.objects
      }

  def prepare(self):
      pass

  def close(self):
      pass

  def ask(self, statement):
      if Parser.is_negative(statement):
          return not self.ask(Parser.negate(statement))

      terms = Parser.logical_split(statement)
      result = []

      handlers = list(filter(lambda pattern: re.match(pattern, statement), self._handlers))
      
      if handlers:
        args = re.findall(handlers[0], statement)
        return self._handlers[handlers[0]](args[0])
        
      if '?' in terms:
          query = {'attributes.key': terms[0]}

          if terms[1] == '?':
            query['object_id'] = terms[2] if terms[2][0] != '!' else { '$ne': terms[2][1:] }
            items = self.objects.find(query)

            for item in items:  
              for attr in item['attributes']:
                  if attr['key'] == terms[0]:
                    result.append(*attr['value'])
            
            return result
            
          if terms[2] == '?':
            query['attributes.value'] = terms[1] if terms[1][0] != '!' else { '$ne': terms[1][1:] }
            items = self.objects.find(query)

            for item in items:
              result.append(item['object_id'])
            return result

      else:
        return self.objects.count_documents({
          'attributes.key': terms[0],
          'attributes.value': terms[1],
          'object_id': terms[2]
        }) > 0
          

  def tell(self, statement):
    if Parser.is_negative(statement):
        terms = Parser.logical_split(Parser.negate(statement))
        self.objects.update({'object_id': terms[2]}, {'$pull': {
          'attributes': {
            'key': terms[0],
            'value': terms[1],
          }
        }}, upsert=True)
    else:
        terms = Parser.logical_split(statement)

        self.objects.update({'object_id': terms[2]}, {'$pull': {
          'attributes': { 'key': terms[0] }
        }})
        
        self.objects.update({'object_id': terms[2]}, {'$push': {
          'attributes': {
            'key': terms[0],
            'value': terms[1],
          }
        }}, upsert=True)
        
  def inverse(self, role_name):
      inverse_name = self.process(self.evaluate('(role-inverse #!:{0})'.format(role_name)))
      if not len(inverse_name):
          return None
      return inverse_name[0]

  def dump(self):
      facts = list()
      items = self.objects.find({})
      for item in items:
        for attr in item['attributes']:
          for value in attr['value']:
            facts.append('({} {} {})'.format(attr['key'], value, item['object_id']))
      
      return set(facts)

  def get_types(self):
    return [item['typename'] for item in self.types.findall({})]

  def clear_types(self):
    result = self.types.delete_many({})
    return result.deleted_count

  def add_type(self, typename, parent=None):
    entry = {
      'typename': typename, 'parent': 
      parent if parent is not None else ''
    }
    
    result = self.types.replace_one({ 'typename': typename }, entry, upsert=True)
    return result.modified_count

  def remove_type(self, typename):
    children = self.get_child_types(typename)
    result = self.types.delete_many({ 'typename': { '$in': children + [typename] } })
    return result.deleted_count

  def get_parent_types(self, typename):
    cursor = self.types.aggregate([
      {'$match': {'typename': typename}}, 
      {'$graphLookup': {
        'from': 'types', 
        'startWith': '$typename', 
        'connectFromField': 'parent', 
        'connectToField': 'typename', 
        'as': 'parents'
      }}
    ])
    
    result = []

    try:
      item = next(cursor)
      for parent in item['parents']:
        if parent['parent']:
          result.append(parent['parent'])
    except StopIteration:
      pass    

    return list(set(result))

  def get_child_types(self, typename):
    cursor = self.types.aggregate([
      {'$match': {'typename': typename}}, 
      {'$graphLookup': {
        'from': 'types', 
        'startWith': '$typename', 
        'connectFromField': 'typename', 
        'connectToField': 'parent', 
        'as': 'children'
      }}])
    result = []

    try:
      item = next(cursor)      
      for child in item['children']:
        result.append(child['typename'])
    except StopIteration:
      pass

    return list(set(result))

  def add_handler(self, key, callback):
    self._handlers[key] = callback

  def clear_handlers(self):
    self._handlers = {}

  def remove_handler(self, key):
    try: 
      del self._handlers[key]
    except KeyError:
      pass

  def get_handlers(self):
    return self._handlers.keys()

  def load(self, collection, filename):
    try:
      with open(filename) as f:
        for item in json.loads(f.read()):
          self._collections[collection].insert_one(item)
      return True
    except FileNotFoundError:
      return False
    except KeyError:
      return False

  def save(self, collection, filename):
    try:
      to_save = []
      for item in self._collections[collection].find({}):
        del item['_id']
        to_save.append(item)
      with open(filename, 'w') as f:
        f.write(json.dumps(to_save, sort_keys=True, indent=4))
      return True
    except FileNotFoundError:
      return False
    except KeyError:
      return False
    pass

  def clear(self, target=None):
    if not target:
      for collection in self._collections:
        self._collections[collection].delete_many({})
    else:
      self._collections[target].delete_many({})