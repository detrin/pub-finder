# Reachability Color Scale Design

## Goal

Make travel-time differences immediately distinguishable over a detailed street map in both light and dark themes.

## Design

- Replace the single-lilac opacity scale with four semantic colors.
- Use mint for journeys within the selected maximum, yellow for the next 15 minutes, orange for the following 15 minutes and red beyond that.
- Render every band at 48 percent opacity so map labels remain readable and color, rather than transparency alone, carries the distinction.
- Update the Shorter and Longer legend swatches to use the first and last colors.
- Keep the existing interpolation, band thresholds, participant selection and map interaction unchanged.

## Verification

- The renderer test must observe four distinct fill colors and one consistent opacity.
- Inspect the populated wide map in light and dark themes.
- Run all Python and JavaScript tests.
