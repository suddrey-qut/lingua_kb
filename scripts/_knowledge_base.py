import re
import copy
import sys
import pymongo

from lingua_pddl.parser import Parser

class MongoKB:
  def __init__(self):
      self.client = None
      self.db = None
      self.objects = None
      self.types = None

  def connect(self):
      self.client = pymongo.MongoClient()
      self.db = self.client.lingua

      self.objects = self.db.objects
      self.types = self.db.types

  def prepare(self):
      pass

  def close(self):
      pass

  def ask(self, statement):
      if Parser.is_negative(statement):
          return not self.ask(Parser.negate(statement))

      terms = Parser.logical_split(statement)
      result = []
            
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
    print('Inserting statement: {}'.format(statement))
    if Parser.is_negative(statement):
        terms = Parser.logical_split(Parser.negate(statement))
        if len(terms) == 2:
            self.evaluate('(forget-concept-assertion {0} #!:{1} #!:{2})'.format(self.abox, terms[1], terms[0]))
        else:
            self.evaluate('(forget-role-assertion {0} #!:{1} #!:{2} #!:{3})'.format(self.abox, terms[2], terms[1], terms[0]))
    else:
        terms = Parser.logical_split(statement)
        if len(terms) == 2:
            self.evaluate('(add-concept-assertion {0} #!:{1} #!:{2})'.format(self.abox, terms[1], terms[0]))
        else:
            if self.evaluate('(feature? #!:{0})'.format(terms[0])) == 'T':
                previous = self.ask('({0} ? {1})'.format(terms[0], terms[2]))

                if previous:
                    self.evaluate('(forget-role-assertion {0} #!:{1} #!:{2} #!:{3})'.format(self.abox, terms[2], previous[0], terms[0]))

            self.evaluate('(add-role-assertion {0} #!:{1} #!:{2} #!:{3})'.format(self.abox, terms[2], terms[1], terms[0]))

  def inverse(self, role_name):
      inverse_name = self.process(self.evaluate('(role-inverse #!:{0})'.format(role_name)))
      if not len(inverse_name):
          return None
      return inverse_name[0]

  # def get_concepts(self):
  #     return list(set(self.process(self.evaluate('(tbox-retrieve (?x ?y) (and (top ?x) (?x ?y has-child)))'))))
  #
  # def get_roles(self):
  #     return list(set(self.process(self.evaluate('(tbox-retrieve (?x ?y) (and (top ?x) (?x ?y has-child)))'))))

  def dump(self):
      facts = list()
      items = self.objects.find({})
      for item in items:
        for attr in item['attributes']:
          for value in attr['value']:
            facts.append('({} {} {})'.format(attr['key'], value, item['object_id']))
      
      return set(facts)

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
    print([
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

  def get_id(self):
    return self.abox

  def get_uri(self):
    return self.evaluate('(get-namespace-prefix {0}|)'.format(re.findall('.*.owl', self.get_id())[0]))[1:-1]

  def get_empty(self):
    cloned = KnowledgeBase()
    cloned.abox = self.evaluate('(clone-abox DEFAULT)')
    KnowledgeBase.current_abox = cloned.abox
    cloned.prepare()
    return cloned