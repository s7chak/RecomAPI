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