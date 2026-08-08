# Venue quality ranking design

**Date:** 2026-08-08

## Goal

Make nearby venue suggestions easier to judge and rank by their rating quality rather than by walking distance alone. Each venue will show a one-decimal Google rating and its review count, including in the expanded list.

## Ranking model

Google provides only an aggregate rating and review count, not individual review scores or their variance. The ranking therefore uses Bayesian smoothing rather than a statistically invalid exact confidence interval.

For a venue with rating `r` and review count `n`:

```
quality_score = (n * r + prior_weight * prior_rating) / (n + prior_weight)
```

Constants:

- `prior_rating = 4.0`, a neutral-good Google Places baseline.
- `prior_weight = 25`, equivalent to 25 synthetic baseline reviews.

This penalizes uncertain small samples without hiding Google’s actual rating. For example, 5.0 from 3 reviews scores 4.11, while 4.8 from 100 reviews scores 4.64. Missing ratings or counts receive no quality score and sort after scored venues.

## Ordering

`order_pubs_for_stop` will remain the single venue ordering function. After it deduplicates and adds walking distance, it will add `quality_score` and sort by:

1. descending `quality_score`;
2. ascending walking distance;
3. descending raw rating;
4. descending review count;
5. place ID for deterministic ordering.

This intentionally makes quality primary, per the requested product decision. Walking distance remains a clear tie-breaker.

## Display

Every venue pill, including items behind “Show more,” will display:

```
4.6★ (23,360)
```

Ratings render to exactly one decimal. A missing rating remains omitted rather than being invented; a known rating with an unavailable count renders `0` only if the API actually supplied zero.

## Testing

Unit tests will cover smoothing behavior, unknown values, deterministic ordering, and the requested high-rating/low-count versus slightly-lower-rating/high-count comparison. Integration/template coverage will assert one-decimal formatting and review counts in both collapsed and expanded venue lists.

## Scope

This changes only ranking and presentation of venues already returned by Google. It does not change Google queries, cache lifetime, result limits, or transit ranking.
