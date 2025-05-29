name: Question
about: Ask a question about Timeplus Proton
labels: question
body:
  - type: markdown
    attributes:
      value: |
        > Make sure to check documentation https://docs.timeplus.com first. If the question is concise and probably has a short answer, asking it in [community Slack](https://timeplus.com/slack) is probably the fastest way to find the answer.
  - type: textarea
    attributes:
      label: Company or project name
      description: Put your company name or project description here.
    validations:
      required: false
  - type: textarea
    attributes:
      label: Question
      description: Please put your question here.
    validations:
      required: true
