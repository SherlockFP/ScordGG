# SCORD Design Bible

> Source of truth for every SCORD screen. The visual reference is the approved
> ImageGen concept `exec-e2f2415d-4c3b-41fd-923b-092594c6a5ef.png`.

## Product character

SCORD is a private, high-signal social space: dark sapphire, calm, precise and
slightly futuristic. It borrows Discord's information density, but uses more
layering, breathing room and translucent surfaces. It must never look like a
flat gray Discord reskin.

## Layout contract

- Desktop shell: 72 px server rail + 260 px navigation/channel panel + fluid
  workspace. Optional directory/member panel is 264-288 px.
- Main surfaces sit inside the ambient canvas with 12-16 px gutters and 18-22 px
  rounded corners. Do not draw every region edge-to-edge.
- Headers are 64 px, translucent and visually attached to their content panel.
- Lists own their scroll. Parent panels do not create a second scrollbar.
- At 900 px, the directory becomes a drawer. At 768 px, rail and navigation
  become an overlay. At 560 px, the product uses one content column.

## Tokens

| Role | Token | Value |
|---|---|---|
| Canvas | `--scord-canvas` | `#050b18` |
| Deep surface | `--scord-surface-0` | `#081225` |
| Main surface | `--scord-surface-1` | `rgba(14, 27, 52, .78)` |
| Raised surface | `--scord-surface-2` | `rgba(22, 39, 70, .72)` |
| Hover surface | `--scord-surface-hover` | `rgba(103, 95, 220, .13)` |
| Border | `--scord-border` | `rgba(139, 167, 226, .14)` |
| Strong border | `--scord-border-strong` | `rgba(129, 112, 255, .34)` |
| Primary text | `--scord-text-1` | `#f2f5ff` |
| Secondary text | `--scord-text-2` | `#a5b1cb` |
| Muted text | `--scord-text-3` | `#71809f` |
| Violet | `--scord-accent` | `#7567ff` |
| Blue | `--scord-blue` | `#4f8cff` |
| Online | `--scord-online` | `#35d49a` |
| Warning | `--scord-warning` | `#f4b84a` |
| Danger | `--scord-danger` | `#f0627e` |

Use Inter for body copy and Space Grotesk for product labels/headings. Body copy
is 14-15 px with 1.5 line height. Section labels are 10-11 px uppercase with
`.12em` tracking.

## Surface language

- Panel: `linear-gradient(145deg, rgba(17,31,58,.86), rgba(7,15,31,.94))`.
- Blur: 16-22 px; use only on rail, headers, drawers and modals.
- Shadow: `0 24px 70px rgba(0,0,0,.28)` for major layers.
- Radius: 10 px control, 14 px row/card, 18 px panel, 22 px modal/stage.
- Accent is reserved for selected navigation, primary actions and focus rings.
  Normal rows remain transparent until hover.

## Screen rules

### Navigation and user card

- The selected destination uses a violet/blue translucent fill and 1 px accent
  border. No oversized glow.
- Channel rows are 42-46 px. Category labels are compact and aligned.
- The user card is a distinct raised surface containing avatar, status, mic,
  deafen and settings. It must fit without clipping or notification buttons.

### Chat

- Message rows are left-aligned for both self and others, like a conversation
  timeline. Self messages may use a subtle author accent, never a filled chat
  bubble.
- Message backgrounds are transparent. Hover may add at most 5% white/violet.
- Avatar, author, time and body form one aligned grid. Grouped messages keep the
  same text start position.
- Composer is a floating translucent bar with 14-18 px radius, not an outlined
  full-width form glued to the viewport.

### Directory / members

- Header says `Dizin`, includes context copy and search.
- Role labels stay sticky within the member list. Rows are transparent, with
  circular avatars and visible presence dots.
- The list has a thin designed scrollbar; the outer panel never scrolls.

### Voice

- Participants render as bounded 16:9 cards inside a glass stage. A single
  participant never expands into an empty full-screen slab.
- Primary leave/share/camera controls sit in one centered floating dock.

### Settings and modals

- Modal scrim is 55% black plus 8 px blur. One scroll owner only.
- Desktop account settings use two columns and fit a 900x700 viewport without
  mandatory scrolling. Short/mobile viewports may scroll.

## Interaction and accessibility

- Controls are at least 40 px desktop / 44 px touch.
- Focus ring: `0 0 0 3px rgba(117,103,255,.22)` plus strong accent border.
- Motion is 150-220 ms using `cubic-bezier(.2,.8,.2,1)`.
- Respect `prefers-reduced-motion`; no infinite decorative animation.
- Primary text maintains 4.5:1 contrast; state is never communicated by color
  alone.

## Forbidden patterns

- Flat opaque gray full-width message cards.
- Right-aligned messenger bubbles for normal channel chat.
- Multiple nested scrollbars.
- Emoji used as a structural control icon.
- Arbitrary per-screen blues/purples outside semantic tokens.
- Borders on every container; hierarchy comes from spacing and surface depth.

## Acceptance views

- 1440x900: server chat, friends directory, voice stage, user settings.
- 1024x768: directory collapses without covering composer.
- 390x844: one content column, overlay navigation, no horizontal overflow.
- No uncaught console errors, no clipped fixed controls and no unexpected
  network 4xx/5xx in the tested flow.
