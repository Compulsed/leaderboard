# Leadering Precursor

# Design Considersations
- Making sure that if the lambda times out or something goes wrong, that a batch of records can be replayed

# Features
- Want to be able to find the rank that user is in
- How do we handle daily scores, when is the cutover period?
- Moving older leaderboards to a slower instance of DDB or shipping them to S3?
    - How does this affect cost? I guess not all of your data needs high RCU
- Location based scores using places local time

# Performance strategies
- Setting dynamodb to 1 RCU/WRU

