from ORM.entity import *
from ORM.db_config import db_config

class Section(Entity):
    _columns  = ['title']
    _parents  = []
    _children = {'categories': 'Category'}
    _siblings = {}

class Category(Entity):
    _columns  = ['title']
    _parents  = ['section']
    _children = {'posts': 'Post'}
    _siblings = {}

class Post(Entity):
    _columns  = ['content', 'title']
    _parents  = ['category']
    _children = {'comments': 'Comment'}
    _siblings = {'tags': 'Tag'}

class Comment(Entity):
    _columns  = ['text']
    _parents  = ['post', 'user']
    _children = {}
    _siblings = {}

class Tag(Entity):
    _columns  = ['name']
    _parents  = []
    _children = {}
    _siblings = {'posts': 'Post'}

class User(Entity):
    _columns  = ['name', 'email', 'age']
    _parents  = []
    _children = {'comments': 'Comment'}
    _siblings = {}


if __name__ == "__main__":
    Entity.db = connect(**db_config)
    section = Section()
    section.title = "zalupa"
    section.save()
    section.title = "zalupa updated"
    section.save()

    category = Category(30)
    category.section = section
    category.title = 'news'
    category.save()
    category.section = Section(5)
    category.save()
    post = Post(2)
    post.title = 'ololo'
    post.category = category
    post.save()
    tag = Tag(1)
    tag.name = 'tag123'
    tag.save()
    post.tags = tag

    for tag in post.tags:
        print(tag.name)

    # for categ in section.categories:
    #     print(categ.title)

    # for section in Category.all():
    #     print(section.title)

    Entity.db.close()
