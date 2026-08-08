# Private Repository and Session Alignment Design

## Scope

- Change `detrin/pub-finder` visibility from public to private on GitHub.
- Remove the repository link from the site footer while retaining the GitHub link in the navigation menu.
- Keep the desktop settings card sticky, but ensure it never extends below the visible people card.

## Layout fix

The desktop grid currently aligns both cards to the start. When settings is taller than the people content, the people card ends early and the settings card extends past it. Stretch the people card to the grid row height. If settings is taller, both card bottoms align. If the people list is taller, it continues to define the sticky card's containing boundary. Mobile remains a single-column, non-sticky layout.

## Verification

- Render the shell and confirm the repository URL is absent from the footer but still present in navigation.
- At desktop width with one participant, confirm the people and settings card bottoms align before and after scrolling.
- At desktop width with a taller people list, confirm settings remains sticky and cannot pass the people card bottom.
- Confirm the mobile single-column layout is unchanged.
- Run all Python and JavaScript tests before pushing `main`.
