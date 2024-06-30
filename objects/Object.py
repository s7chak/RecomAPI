class Book:
    def __init__(self, title, people, summary, date, props):
        self.title = title
        self.people = people
        self.summary = summary
        self.date = date
        self.props = props

    def __repr__(self):
        return f"Book (title={self.title}, people={self.people}, date={self.date}, props={self.props})"

    def to_dict(self):
        return {
            'Title': self.title,
            'People': self.people,
            'Summary': self.summary,
            'Date': self.date,
            'Parameters': self.props
            }

class Paper:
    def __init__(self, title, link, authors=None, date=None, summary=None):
        self.title = title
        self.date = date
        self.link = link
        self.authors = authors
        self.summary = summary

    def __repr__(self):
        return f"Paper (title={self.title}, link={self.link}, people={self.people}, date={self.date}, props={self.props})"

    def to_dict(self):
        return {
            'Title': self.title,
            'Link': self.link,
            'People': self.people,
            'Summary': self.summary,
            'Date': self.date,
            'Parameters': self.props
        }