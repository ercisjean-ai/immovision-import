const currencyFormat = new Intl.NumberFormat("fr-BE", {
  style: "currency",
  currency: "EUR",
  maximumFractionDigits: 0,
});

const numberFormat = new Intl.NumberFormat("fr-BE", {
  maximumFractionDigits: 0,
});

const sectionConfig = [
  {
    key: "new",
    title: "Nouveau (< 7 jours)",
    subtitle: "Biens apparus recemment et a traiter avant qu'ils disparaissent du radar.",
  },
  {
    key: "compatible",
    title: "Compatible",
    subtitle: "Biens alignes avec la strategie investisseur et a prioriser.",
  },
  {
    key: "a_analyser",
    title: "A analyser",
    subtitle: "Biens prometteurs avec prudences ou points a confirmer.",
  },
  {
    key: "hors_criteres",
    title: "Hors criteres",
    subtitle: "Biens hors cadre selon la strategie actuelle.",
  },
  {
    key: "modified",
    title: "Modifies",
    subtitle: "Dernieres observations ayant bouge de facon utile.",
  },
];

const defaultViewState = Object.freeze({
  liveOnly: true,
  search: "",
  source: "all",
  strategy: "all",
  observation: "all",
  zone: "all",
  sort: "priority",
});

const dashboardModeStorageKey = "immovision-dashboard-live-only";
const strictPriorityPropertyTypes = new Set(["apartment_block", "commercial_house"]);
const strictAcceptedPropertyTypes = new Set([
  "apartment_block",
  "commercial_house",
  "commercial",
  "mixed_use",
]);
const strictTargetZones = new Set(["Bruxelles cible", "Peripherie cible"]);

const dashboardState = {
  payload: null,
  listings: [],
  secondaryReview: [],
  view: { ...defaultViewState },
};

document.addEventListener("DOMContentLoaded", async () => {
  bindControls();

  const liveOnlyToggle = document.getElementById("live-only-toggle");
  dashboardState.view.liveOnly = readPersistedLiveOnlyMode(
    liveOnlyToggle?.checked ?? true,
  );
  setControlValue("live-only-toggle", dashboardState.view.liveOnly);

  await loadDashboard({ liveOnly: dashboardState.view.liveOnly });
});

function bindControls() {
  const liveOnlyToggle = document.getElementById("live-only-toggle");
  liveOnlyToggle?.addEventListener("change", async () => {
    dashboardState.view.liveOnly = liveOnlyToggle.checked;
    persistLiveOnlyMode(dashboardState.view.liveOnly);
    await loadDashboard({ liveOnly: liveOnlyToggle.checked });
  });

  bindValueControl("search-input", "input", (value) => {
    dashboardState.view.search = value;
    renderCurrentView();
  });
  bindValueControl("source-filter", "change", (value) => {
    dashboardState.view.source = value;
    renderCurrentView();
  });
  bindValueControl("strategy-filter", "change", (value) => {
    dashboardState.view.strategy = value;
    renderCurrentView();
  });
  bindValueControl("observation-filter", "change", (value) => {
    dashboardState.view.observation = value;
    renderCurrentView();
  });
  bindValueControl("zone-filter", "change", (value) => {
    dashboardState.view.zone = value;
    renderCurrentView();
  });
  bindValueControl("sort-select", "change", (value) => {
    dashboardState.view.sort = value;
    renderCurrentView();
  });

  document.getElementById("reset-filters")?.addEventListener("click", () => {
    dashboardState.view.search = "";
    dashboardState.view.source = "all";
    dashboardState.view.strategy = "all";
    dashboardState.view.observation = "all";
    dashboardState.view.zone = "all";
    dashboardState.view.sort = defaultViewState.sort;
    syncFilterControlValues();
    renderCurrentView();
  });
}

function bindValueControl(id, eventName, onChange) {
  const element = document.getElementById(id);
  element?.addEventListener(eventName, () => {
    onChange(readControlValue(element));
  });
}

async function loadDashboard({ liveOnly }) {
  try {
    const requestedLiveOnly = Boolean(liveOnly);
    const query = requestedLiveOnly ? "?live_only=1" : "";
    const response = await fetch(`/api/dashboard${query}`, {
      cache: "no-store",
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Impossible de charger le dashboard.");
    }

    const safePayload = asObject(payload);
    dashboardState.payload = safePayload;
    dashboardState.view.liveOnly = requestedLiveOnly;
    dashboardState.payload.live_only = requestedLiveOnly;

    const preparedListings = asArray(safePayload.listings).map((item) =>
      prepareListing(item, { strictPayload: requestedLiveOnly }),
    );
    const preparedSecondaryReview = asArray(safePayload.secondary_review).map((item) =>
      prepareListing(item, { strictPayload: false }),
    );
    dashboardState.listings = preparedListings;
    dashboardState.secondaryReview = preparedSecondaryReview;

    syncFilterControlValues();
    populateFilterOptions([
      ...dashboardState.listings,
      ...dashboardState.secondaryReview,
    ]);
    renderCurrentView();
  } catch (error) {
    renderError(error);
  }
}

function renderCurrentView() {
  const payload = asObject(dashboardState.payload);
  const databasePath = document.getElementById("database-path");
  const generatedAt = document.getElementById("generated-at");
  const dataModeHint = document.getElementById("data-mode-hint");

  databasePath.textContent = nonEmptyText(payload.database_path, "-");
  generatedAt.textContent = formatDateTime(payload.generated_at);
  dataModeHint.textContent = buildDataModeHint(payload);

  const preparedView = buildPreparedView();

  renderResultStats(preparedView);
  renderPriorityPanel(preparedView.priorityItems);
  renderSecondaryReviewPanel(preparedView.secondaryReviewItems);
  renderSummary(document.getElementById("summary-grid"), preparedView.summary);
  renderSections(
    document.getElementById("sections"),
    preparedView.sections,
    preparedView.priorityIds,
  );
}

function prepareListing(item, options = {}) {
  const safeItem = asObject(item);
  const strictPayload = Boolean(options.strictPayload);
  const searchableText = normalizeText(
    [
      safeItem.title,
      safeItem.display_location,
      safeItem.commune,
      safeItem.postal_code,
      safeItem.display_source,
      safeItem.source_listing_id,
      safeItem.zone_label,
      safeItem.strategy_label,
      safeItem.observation_status,
      safeItem.compatibility_reason,
    ]
      .filter(Boolean)
      .join(" "),
  );

  return {
    ...safeItem,
    is_live_eligible: normalizeBooleanLike(
      safeItem.is_live_eligible,
      strictPayload,
    ),
    is_dashboard_eligible: normalizeBooleanLike(
      safeItem.is_dashboard_eligible,
      strictPayload,
    ),
    _searchText: searchableText,
    _sourceFilterValue: nonEmptyText(
      safeItem.display_source,
      nonEmptyText(safeItem.source_name, "Source"),
    ),
    _zoneFilterValue: nonEmptyText(safeItem.zone_label, "Zone inconnue"),
    _observationFilterValue: nonEmptyText(safeItem.observation_status, "seen"),
    _strategyFilterValue: nonEmptyText(safeItem.strategy_label, "A analyser"),
    _freshnessTimestamp: parseDateValue(safeItem.last_seen_at || safeItem.first_seen_at),
  };
}

function buildPreparedView() {
  const filteredListings = applyFilters(dashboardState.listings, dashboardState.view);
  const filteredSecondaryReview = applyFilters(
    dashboardState.secondaryReview,
    dashboardState.view,
    { bypassStrictEligibility: true },
  );
  const sortedListings = sortListings(filteredListings, dashboardState.view.sort);
  const sortedSecondaryReview = sortListings(
    filteredSecondaryReview,
    dashboardState.view.sort,
  );
  const priorityItems = buildPriorityItems(sortedListings);
  const priorityIds = new Set(priorityItems.map((item) => item.listing_id));

  return {
    sourceCount: dashboardState.listings.length,
    filteredCount: sortedListings.length,
    activeFilters: buildActiveFilters(),
    summary: buildVisibleSummary(
      sortedListings,
      priorityIds,
      sortedSecondaryReview.length,
    ),
    priorityItems,
    priorityIds,
    secondaryReviewItems: sortedSecondaryReview.slice(0, 8),
    sections: buildSections(sortedListings),
  };
}

function applyFilters(listings, view, options = {}) {
  const bypassStrictEligibility = Boolean(options.bypassStrictEligibility);
  return listings.filter((item) => {
    if (
      view.liveOnly &&
      !bypassStrictEligibility &&
      item.is_dashboard_eligible !== true
    ) {
      return false;
    }
    if (view.source !== "all" && item._sourceFilterValue !== view.source) {
      return false;
    }
    if (view.strategy !== "all" && item._strategyFilterValue !== view.strategy) {
      return false;
    }
    if (
      view.observation !== "all" &&
      item._observationFilterValue !== view.observation
    ) {
      return false;
    }
    if (view.zone !== "all" && item._zoneFilterValue !== view.zone) {
      return false;
    }
    if (view.search) {
      const needle = normalizeText(view.search);
      if (!item._searchText.includes(needle)) {
        return false;
      }
    }
    return true;
  });
}

function sortListings(listings, sortKey) {
  const items = [...listings];
  items.sort((left, right) => compareListings(left, right, sortKey));
  return items;
}

function compareListings(left, right, sortKey) {
  if (sortKey === "investment_desc") {
    return compareNumbersDesc(
      left.investment_score,
      right.investment_score,
      left,
      right,
    );
  }
  if (sortKey === "confidence_desc") {
    return compareNumbersDesc(
      left.confidence_score,
      right.confidence_score,
      left,
      right,
    );
  }
  if (sortKey === "price_asc") {
    return compareNumbersAsc(left.price, right.price, left, right);
  }
  if (sortKey === "ppu_asc") {
    return compareNumbersAsc(
      left.price_per_unit,
      right.price_per_unit,
      left,
      right,
    );
  }
  if (sortKey === "newest") {
    return compareFreshness(left, right);
  }
  return comparePriority(left, right);
}

function comparePriority(left, right) {
  const delta = computePriorityScore(right) - computePriorityScore(left);
  if (delta !== 0) {
    return delta;
  }
  return compareFreshness(left, right);
}

function computePriorityScore(item) {
  let score = 0;
  if (item.is_dashboard_eligible === true) {
    score += 320;
  } else if (item.matches_investor_criteria === true) {
    score += 140;
  } else {
    score -= 420;
  }

  if (item.strategy_label === "Compatible") {
    score += 170;
  } else if (item.strategy_label === "A analyser") {
    score += 55;
  } else {
    score -= 240;
  }

  const propertyType = nonEmptyText(item.property_type);
  if (strictPriorityPropertyTypes.has(propertyType)) {
    score += 180;
  } else if (strictAcceptedPropertyTypes.has(propertyType)) {
    score += 80;
  } else if (propertyType === "house" || propertyType === "apartment") {
    score -= 260;
  } else {
    score -= 180;
  }

  if (item.zone_label === "Bruxelles cible") {
    score += 120;
  } else if (item.zone_label === "Peripherie cible") {
    score += 105;
  } else if (item.zone_label === "Zone a analyser") {
    score -= 220;
  } else {
    score -= 260;
  }

  const units = toFiniteNumber(item.existing_units) ?? 0;
  if (units >= 4) {
    score += 90;
  } else if (units === 3) {
    score += 75;
  } else if (units === 2) {
    score += 55;
  } else {
    score -= 260;
  }

  const pricePerUnit = toFiniteNumber(item.price_per_unit);
  if (pricePerUnit == null) {
    score -= 120;
  } else if (pricePerUnit <= 120000) {
    score += 30;
  } else if (pricePerUnit <= 150000) {
    score += 22;
  } else if (pricePerUnit <= 170000) {
    score += 12;
  } else {
    score -= 180;
  }

  if (item.copro_status === "false") {
    score += 26;
  } else if (item.copro_status === "unknown") {
    score -= 18;
  } else if (item.copro_status === "true") {
    score -= 260;
  }

  if (item.is_new) {
    score += 36;
  }
  if (item.observation_status === "modified") {
    score += 12;
  }

  if (item.source_name === "Biddit" || item.source_name === "Notaire.be") {
    score -= 18;
  }

  const investment = toFiniteNumber(item.investment_score) ?? 0;
  const confidence = toFiniteNumber(item.confidence_score) ?? 0;
  score += investment * 0.8;
  score += confidence * 0.35;

  if (!item.source_url_valid) {
    score -= 300;
  }
  return score;
}

function isPriorityCandidate(item) {
  if (item.is_dashboard_eligible !== true) {
    return false;
  }
  if (!strictTargetZones.has(nonEmptyText(item.zone_label))) {
    return false;
  }
  if (!strictAcceptedPropertyTypes.has(nonEmptyText(item.property_type))) {
    return false;
  }
  const units = toFiniteNumber(item.existing_units);
  if (units == null || units < 2) {
    return false;
  }
  const pricePerUnit = toFiniteNumber(item.price_per_unit);
  if (pricePerUnit == null || pricePerUnit > 170000) {
    return false;
  }
  return true;
}

function compareFreshness(left, right) {
  const freshnessDelta = right._freshnessTimestamp - left._freshnessTimestamp;
  if (freshnessDelta !== 0) {
    return freshnessDelta;
  }
  return compareNumbersDesc(
    left.investment_score,
    right.investment_score,
    left,
    right,
  );
}

function compareNumbersDesc(leftValue, rightValue, leftItem, rightItem) {
  const leftNumber = toFiniteNumber(leftValue);
  const rightNumber = toFiniteNumber(rightValue);
  if (leftNumber == null && rightNumber == null) {
    return compareText(leftItem.title, rightItem.title);
  }
  if (leftNumber == null) {
    return 1;
  }
  if (rightNumber == null) {
    return -1;
  }
  if (rightNumber !== leftNumber) {
    return rightNumber - leftNumber;
  }
  return compareFreshnessFallback(leftItem, rightItem);
}

function compareNumbersAsc(leftValue, rightValue, leftItem, rightItem) {
  const leftNumber = toFiniteNumber(leftValue);
  const rightNumber = toFiniteNumber(rightValue);
  if (leftNumber == null && rightNumber == null) {
    return compareText(leftItem.title, rightItem.title);
  }
  if (leftNumber == null) {
    return 1;
  }
  if (rightNumber == null) {
    return -1;
  }
  if (leftNumber !== rightNumber) {
    return leftNumber - rightNumber;
  }
  return compareFreshnessFallback(leftItem, rightItem);
}

function compareFreshnessFallback(leftItem, rightItem) {
  const freshnessDelta =
    rightItem._freshnessTimestamp - leftItem._freshnessTimestamp;
  if (freshnessDelta !== 0) {
    return freshnessDelta;
  }
  return compareText(leftItem.title, rightItem.title);
}

function compareText(left, right) {
  return nonEmptyText(left).localeCompare(nonEmptyText(right), "fr", {
    sensitivity: "base",
  });
}

function buildPriorityItems(listings) {
  return listings
    .filter((item) => isPriorityCandidate(item))
    .slice()
    .sort(comparePriority)
    .slice(0, 4)
    .map((item, index) => ({
      ...item,
      is_priority: true,
      priority_label: index === 0 ? "Top pick" : "Prioritaire",
    }));
}

function buildVisibleSummary(listings, priorityIds, secondaryReviewCount = 0) {
  return {
    visible: listings.length,
    priority: priorityIds.size,
    secondary_review_total: secondaryReviewCount,
    compatible: listings.filter(
      (item) => item.strategy_label === "Compatible",
    ).length,
    a_analyser: listings.filter(
      (item) => item.strategy_label === "A analyser",
    ).length,
    hors_criteres: listings.filter(
      (item) => item.strategy_label === "Hors criteres",
    ).length,
    new: listings.filter((item) => item.is_new).length,
    modified: listings.filter((item) => item.is_modified).length,
    valid_links: listings.filter((item) => item.source_url_valid).length,
  };
}

function buildSections(listings) {
  return {
    new: listings.filter((item) => item.is_new),
    compatible: listings.filter(
      (item) => item.strategy_label === "Compatible",
    ),
    a_analyser: listings.filter(
      (item) => item.strategy_label === "A analyser",
    ),
    hors_criteres: listings.filter(
      (item) => item.strategy_label === "Hors criteres",
    ),
    modified: listings.filter((item) => item.is_modified),
  };
}

function buildActiveFilters() {
  const filters = [];
  if (dashboardState.view.search) {
    filters.push(`Recherche: ${dashboardState.view.search}`);
  }
  if (dashboardState.view.source !== "all") {
    filters.push(`Source: ${dashboardState.view.source}`);
  }
  if (dashboardState.view.strategy !== "all") {
    filters.push(`Strategie: ${dashboardState.view.strategy}`);
  }
  if (dashboardState.view.observation !== "all") {
    filters.push(`Observation: ${dashboardState.view.observation}`);
  }
  if (dashboardState.view.zone !== "all") {
    filters.push(`Zone: ${dashboardState.view.zone}`);
  }
  if (dashboardState.view.sort !== defaultViewState.sort) {
    filters.push(`Tri: ${displaySortLabel(dashboardState.view.sort)}`);
  }
  return filters;
}

function populateFilterOptions(listings) {
  setSelectOptions(
    "source-filter",
    "Toutes",
    collectDistinctOptions(listings, "_sourceFilterValue"),
  );
  setSelectOptions(
    "strategy-filter",
    "Toutes",
    collectOrderedOptions(listings, "_strategyFilterValue", [
      "Compatible",
      "A analyser",
      "Hors criteres",
    ]),
  );
  setSelectOptions(
    "observation-filter",
    "Toutes",
    collectOrderedOptions(listings, "_observationFilterValue", [
      "new",
      "modified",
      "seen",
    ]),
  );
  setSelectOptions(
    "zone-filter",
    "Toutes",
    collectOrderedOptions(listings, "_zoneFilterValue", [
      "Bruxelles cible",
      "Peripherie cible",
      "Zone a analyser",
      "Zone inconnue",
    ]),
  );
  syncFilterControlValues();
}

function collectDistinctOptions(listings, fieldName) {
  return [
    ...new Set(
      listings.map((item) => nonEmptyText(item[fieldName])).filter(Boolean),
    ),
  ]
    .sort((left, right) =>
      left.localeCompare(right, "fr", { sensitivity: "base" }),
    )
    .map((value) => ({ value, label: value }));
}

function collectOrderedOptions(listings, fieldName, preferredOrder) {
  const distinct = new Set(
    listings.map((item) => nonEmptyText(item[fieldName])).filter(Boolean),
  );
  const preferred = preferredOrder
    .filter((value) => distinct.has(value))
    .map((value) => ({
      value,
      label: value,
    }));
  const remaining = [...distinct]
    .filter((value) => !preferredOrder.includes(value))
    .sort((left, right) =>
      left.localeCompare(right, "fr", { sensitivity: "base" }),
    )
    .map((value) => ({ value, label: value }));
  return [...preferred, ...remaining];
}

function setSelectOptions(selectId, allLabel, options) {
  const select = document.getElementById(selectId);
  if (!select) {
    return;
  }

  const currentValue = readControlValue(select);
  const fragment = document.createDocumentFragment();
  fragment.appendChild(createOption("all", allLabel));
  options.forEach((option) => {
    fragment.appendChild(createOption(option.value, option.label));
  });
  select.replaceChildren(fragment);

  if (
    currentValue &&
    (currentValue === "all" ||
      options.some((option) => option.value === currentValue))
  ) {
    select.value = currentValue;
  } else {
    select.value = "all";
    const stateKey = selectId.replace("-filter", "").replace("-select", "");
    if (stateKey === "source") {
      dashboardState.view.source = "all";
    }
    if (stateKey === "strategy") {
      dashboardState.view.strategy = "all";
    }
    if (stateKey === "observation") {
      dashboardState.view.observation = "all";
    }
    if (stateKey === "zone") {
      dashboardState.view.zone = "all";
    }
  }
}

function createOption(value, label) {
  const option = document.createElement("option");
  option.value = value;
  option.textContent = label;
  return option;
}

function syncFilterControlValues() {
  setControlValue("live-only-toggle", dashboardState.view.liveOnly);
  setControlValue("search-input", dashboardState.view.search);
  setControlValue("source-filter", dashboardState.view.source);
  setControlValue("strategy-filter", dashboardState.view.strategy);
  setControlValue("observation-filter", dashboardState.view.observation);
  setControlValue("zone-filter", dashboardState.view.zone);
  setControlValue("sort-select", dashboardState.view.sort);
}

function setControlValue(id, value) {
  const element = document.getElementById(id);
  if (!element) {
    return;
  }
  if (element.type === "checkbox") {
    element.checked = Boolean(value);
    return;
  }
  element.value = value;
}

function readControlValue(element) {
  if (!element) {
    return "";
  }
  if (element.type === "checkbox") {
    return element.checked;
  }
  return element.value;
}

function renderResultStats(preparedView) {
  const payload = asObject(dashboardState.payload);
  const summary = asObject(payload.summary);
  const visibleCount = document.getElementById("visible-count");
  const priorityCount = document.getElementById("priority-count");
  const resultScope = document.getElementById("result-scope");

  if (dashboardState.view.liveOnly) {
    visibleCount.textContent = `${numberFormat.format(
      preparedView.filteredCount,
    )} annonce(s) exploitable(s) affichee(s)`;
    priorityCount.textContent = `${numberFormat.format(
      summary.non_live || 0,
    )} exclue(s) car non live`;

    const parts = [
      `${numberFormat.format(
        summary.live_with_invalid_link || 0,
      )} exclue(s) car lien invalide`,
      `${numberFormat.format(
        summary.live_inactive_or_closed || 0,
      )} exclue(s) car vente inactive/cloturee`,
      `${numberFormat.format(
        summary.live_out_of_criteria || 0,
      )} exclue(s) car hors criteres`,
      `${numberFormat.format(
        summary.secondary_review_total || 0,
      )} en analyse serieuse`,
    ];
    if (preparedView.activeFilters.length > 0) {
      parts.push(preparedView.activeFilters.join(" | "));
    } else {
      parts.push("Vue investisseur stricte active.");
    }
    resultScope.textContent = parts.join(" | ");
  } else {
    visibleCount.textContent = `${numberFormat.format(
      preparedView.filteredCount,
    )} annonce(s) en mode debug`;
    priorityCount.textContent = `${numberFormat.format(
      summary.non_live || 0,
    )} non live dans la base`;

    if (preparedView.activeFilters.length > 0) {
      resultScope.textContent = `Mode debug actif | ${preparedView.activeFilters.join(" | ")}`;
    } else {
      resultScope.textContent = `Mode debug actif | ${numberFormat.format(
        summary.live_with_invalid_link || 0,
      )} annonce(s) live au lien invalide visibles hors vue principale`;
    }
  }
}

function renderPriorityPanel(priorityItems) {
  const root = document.getElementById("priority-panel");
  if (!root) {
    return;
  }

  root.replaceChildren();
  if (!priorityItems.length) {
    root.hidden = true;
    return;
  }

  root.hidden = false;
  const panel = document.createElement("section");
  panel.className = "panel panel-priority";

  const head = document.createElement("div");
  head.className = "panel-head";
  head.innerHTML = `
    <div>
      <h2 class="panel-title">A regarder d'abord</h2>
      <p class="panel-subtitle">Selection rapide des biens les plus exploitables selon score, confiance, fraicheur et strategie.</p>
    </div>
    <span class="panel-count">${numberFormat.format(priorityItems.length)} biens</span>
  `;

  const grid = document.createElement("div");
  grid.className = "listing-grid priority-grid";
  grid.replaceChildren(
    ...priorityItems.map((item) =>
      renderListingCard({
        ...item,
        is_priority: true,
        priority_label: item.priority_label || "Prioritaire",
      }),
    ),
  );

  panel.append(head, grid);
  root.append(panel);
}

function renderSecondaryReviewPanel(items) {
  const root = document.getElementById("secondary-review-panel");
  if (!root) {
    return;
  }

  root.replaceChildren();
  if (!dashboardState.view.liveOnly || !items.length) {
    root.hidden = true;
    return;
  }

  root.hidden = false;
  const panel = document.createElement("section");
  panel.className = "panel panel-secondary-review";

  const head = document.createElement("div");
  head.className = "panel-head";
  head.innerHTML = `
    <div>
      <h2 class="panel-title">A analyser serieusement</h2>
      <p class="panel-subtitle">Biens live, lien valide et vente active, mais avec 1 ou 2 points non bloquants a verifier avant de les considerer comme exploitables stricts.</p>
    </div>
    <span class="panel-count">${numberFormat.format(items.length)} biens</span>
  `;

  const grid = document.createElement("div");
  grid.className = "listing-grid secondary-review-grid";
  grid.replaceChildren(
    ...items.map((item) =>
      renderListingCard({
        ...item,
        display_reason: nonEmptyText(
          item.secondary_review_reason,
          item.compatibility_reason,
        ),
      }),
    ),
  );

  panel.append(head, grid);
  root.append(panel);
}

function renderSummary(root, summary) {
  const template = document.getElementById("summary-card-template");
  const cards = [
    ["Visibles", summary.visible],
    ["Prioritaires", summary.priority],
    ["A analyser serieusement", summary.secondary_review_total || 0],
    ["Compatibles", summary.compatible],
    ["A analyser", summary.a_analyser],
    ["Hors criteres", summary.hors_criteres],
    ["Nouveaux", summary.new],
    ["Modifies", summary.modified],
    ["Liens valides", summary.valid_links],
  ];

  root.replaceChildren(
    ...cards.map(([label, value]) => {
      const node = template.content.firstElementChild.cloneNode(true);
      node.querySelector(".summary-label").textContent = label;
      node.querySelector(".summary-value").textContent = numberFormat.format(
        value,
      );
      return node;
    }),
  );
}

function renderSections(root, sections, priorityIds) {
  const template = document.getElementById("section-template");
  root.replaceChildren(
    ...sectionConfig.map((config) => {
      const node = template.content.firstElementChild.cloneNode(true);
      const items = asArray(sections[config.key]).map((item) => ({
        ...item,
        is_priority: priorityIds.has(item.listing_id),
        priority_label: "Prioritaire",
      }));

      addOptionalClasses(node, sectionClassName(config.key));
      node.querySelector(".panel-title").textContent = config.title;
      node.querySelector(".panel-subtitle").textContent = config.subtitle;
      node.querySelector(".panel-count").textContent = `${numberFormat.format(
        items.length,
      )} biens`;

      const grid = node.querySelector(".listing-grid");
      const emptyState = node.querySelector(".empty-state");
      if (items.length === 0) {
        grid.remove();
      } else {
        emptyState.remove();
        grid.replaceChildren(...items.map(renderListingCard));
      }
      return node;
    }),
  );
}

function renderListingCard(item) {
  const safeItem = asObject(item);
  const template = document.getElementById("listing-card-template");
  const node = template.content.firstElementChild.cloneNode(true);

  addOptionalClasses(
    node,
    listingStrategyClassName(safeItem.strategy_label),
    listingObservationClassName(safeItem.observation_status),
    listingDataOriginClassName(safeItem.data_origin),
    listingLinkClassName(safeItem.source_url_valid),
    safeItem.is_priority ? "listing-priority" : "",
  );

  const priorityBadge = node.querySelector(".priority-badge");
  if (safeItem.is_priority) {
    priorityBadge.textContent = nonEmptyText(
      safeItem.priority_label,
      "Prioritaire",
    );
    addOptionalClasses(priorityBadge, "priority-top");
  } else {
    priorityBadge.remove();
  }

  const sourceBadge = node.querySelector(".source-badge");
  sourceBadge.textContent = nonEmptyText(
    safeItem.display_source,
    nonEmptyText(safeItem.source_name, "Source"),
  );
  addOptionalClasses(sourceBadge, sourceClassName(safeItem.source_name));

  const originBadge = node.querySelector(".origin-badge");
  originBadge.textContent = nonEmptyText(
    safeItem.data_origin_label,
    "Origine inconnue",
  );
  addOptionalClasses(originBadge, dataOriginClassName(safeItem.data_origin));

  const strategyBadge = node.querySelector(".strategy-badge");
  const strategyLabel = nonEmptyText(safeItem.strategy_label, "A analyser");
  strategyBadge.textContent = strategyLabel;
  addOptionalClasses(strategyBadge, strategyClassName(strategyLabel));

  const linkBadge = node.querySelector(".link-badge");
  linkBadge.textContent = safeItem.source_url_valid
    ? "Lien valide"
    : nonEmptyText(safeItem.source_url_issue, "Lien douteux");
  addOptionalClasses(linkBadge, linkClassName(safeItem.source_url_valid));

  const observationBadge = node.querySelector(".observation-badge");
  const observationStatus = nonEmptyText(
    safeItem.observation_status,
    "seen",
  );
  observationBadge.textContent = observationStatus;
  addOptionalClasses(
    observationBadge,
    observationClassName(observationStatus),
  );

  const link = node.querySelector(".open-link");
  if (safeItem.source_url_valid && hasText(safeItem.source_url)) {
    link.href = String(safeItem.source_url).trim();
  } else {
    link.removeAttribute("href");
    link.setAttribute("aria-disabled", "true");
    link.textContent = "Lien indisponible";
    addOptionalClasses(link, "open-link-disabled");
  }

  node.querySelector(".listing-title").textContent = nonEmptyText(
    safeItem.title,
    "Annonce sans titre",
  );
  node.querySelector(".listing-location").textContent = nonEmptyText(
    safeItem.display_location,
    "Localisation a confirmer",
  );
  node.querySelector(".metric-price").textContent = formatCurrency(
    safeItem.price,
  );
  node.querySelector(".metric-units").textContent = formatUnits(
    safeItem.existing_units,
  );
  node.querySelector(".metric-ppu").textContent = formatCurrency(
    safeItem.price_per_unit,
  );
  node.querySelector(".metric-investment").textContent = formatScore(
    safeItem.investment_score,
    safeItem.investment_score_label,
  );
  node.querySelector(".metric-confidence").textContent = formatScore(
    safeItem.confidence_score,
    safeItem.confidence_label,
  );
  node.querySelector(".metric-last-seen").textContent = nonEmptyText(
    safeItem.display_last_seen,
    nonEmptyText(safeItem.display_first_seen, "-"),
  );
  node.querySelector(".listing-reason").textContent = nonEmptyText(
    safeItem.display_reason,
    nonEmptyText(
      safeItem.compatibility_reason,
      nonEmptyText(
        safeItem.secondary_review_reason,
        "Aucun commentaire d'analyse disponible.",
      ),
    ),
  );

  return node;
}

function renderError(error) {
  const summaryGrid = document.getElementById("summary-grid");
  const sectionsRoot = document.getElementById("sections");
  const priorityRoot = document.getElementById("priority-panel");
  const secondaryRoot = document.getElementById("secondary-review-panel");
  const message = document.createElement("section");
  message.className = "panel";
  message.innerHTML = `
    <div class="panel-head">
      <div>
        <h2 class="panel-title">Dashboard indisponible</h2>
        <p class="panel-subtitle">${escapeHtml(error.message || String(error))}</p>
      </div>
    </div>
    <p class="empty-state">Verifie la presence de la base SQLite et relance \`python import.py\` si necessaire.</p>
  `;
  priorityRoot?.replaceChildren();
  if (priorityRoot) {
    priorityRoot.hidden = true;
  }
  secondaryRoot?.replaceChildren();
  if (secondaryRoot) {
    secondaryRoot.hidden = true;
  }
  summaryGrid.replaceChildren();
  sectionsRoot.replaceChildren(message);
}

function buildDataModeHint(payload) {
  const safePayload = asObject(payload);
  const summary = asObject(safePayload.summary);
  const invalidLiveLinks = summary.live_with_invalid_link || 0;
  const inactiveClosed = summary.live_inactive_or_closed || 0;
  const outOfCriteria = summary.live_out_of_criteria || 0;
  const secondaryReview = summary.secondary_review_total || 0;
  if (safePayload.live_only) {
    return `Vue investisseur stricte: seules les annonces live, au lien fiable, encore exploitables et compatibles avec les criteres principaux sont affichees. ${numberFormat.format(summary.non_live || 0)} annonce(s) non live sont exclues, ${numberFormat.format(invalidLiveLinks)} annonce(s) live au lien douteux sont retirees, ${numberFormat.format(inactiveClosed)} vente(s) inactive(s) sont masquees, ${numberFormat.format(outOfCriteria)} bien(s) hors fit investisseur sont exclus et ${numberFormat.format(secondaryReview)} bien(s) restent disponibles dans la vue secondaire d'analyse serieuse.`;
  }
  return `Mode debug: toutes les origines peuvent apparaitre. La vue principale investisseur reste strictement reservee au live avec lien fiable.`;
}

function displaySortLabel(sortKey) {
  return {
    priority: "Priorite investisseur",
    newest: "Fraicheur",
    investment_desc: "Score investissement",
    confidence_desc: "Score confiance",
    price_asc: "Prix total",
    ppu_asc: "Prix / unite",
  }[sortKey] || "Priorite investisseur";
}

function formatCurrency(value) {
  const number = toFiniteNumber(value);
  return number == null ? "-" : currencyFormat.format(number);
}

function formatUnits(value) {
  const number = toFiniteNumber(value);
  if (number == null) {
    return "-";
  }
  return numberFormat.format(number);
}

function formatScore(score, label) {
  const number = toFiniteNumber(score);
  if (number == null) {
    return "-";
  }
  const rounded = Math.round(number);
  return label ? `${rounded} (${label})` : `${rounded}`;
}

function formatDateTime(value) {
  if (!value) {
    return "-";
  }
  try {
    return new Intl.DateTimeFormat("fr-BE", {
      dateStyle: "short",
      timeStyle: "short",
    }).format(new Date(value));
  } catch {
    return value;
  }
}

function strategyClassName(value) {
  if (value === "Compatible") {
    return "strategy-compatible";
  }
  if (value === "Hors criteres") {
    return "strategy-hors";
  }
  return "strategy-analyser";
}

function observationClassName(value) {
  if (value === "new") {
    return "observation-new";
  }
  if (value === "modified") {
    return "observation-modified";
  }
  return "observation-seen";
}

function sectionClassName(value) {
  if (value === "new") {
    return "panel-new";
  }
  if (value === "compatible") {
    return "panel-compatible";
  }
  if (value === "a_analyser") {
    return "panel-analyser";
  }
  if (value === "hors_criteres") {
    return "panel-hors";
  }
  if (value === "modified") {
    return "panel-modified";
  }
  return "";
}

function sourceClassName(value) {
  const normalized = nonEmptyText(value).toLowerCase();
  if (normalized === "biddit") {
    return "source-biddit";
  }
  if (normalized === "immoweb") {
    return "source-immoweb";
  }
  if (normalized === "notaire.be") {
    return "source-notaire";
  }
  if (normalized === "immovlan") {
    return "source-immovlan";
  }
  return "source-generic";
}

function dataOriginClassName(value) {
  if (value === "live") {
    return "origin-live";
  }
  if (value === "fixture") {
    return "origin-fixture";
  }
  if (value === "seed") {
    return "origin-seed";
  }
  if (value === "test") {
    return "origin-test";
  }
  if (value === "file_feed") {
    return "origin-file-feed";
  }
  return "origin-unknown";
}

function listingStrategyClassName(value) {
  if (value === "Compatible") {
    return "listing-compatible";
  }
  if (value === "Hors criteres") {
    return "listing-hors";
  }
  return "listing-analyser";
}

function listingObservationClassName(value) {
  if (value === "new") {
    return "listing-new";
  }
  if (value === "modified") {
    return "listing-modified";
  }
  return "";
}

function listingDataOriginClassName(value) {
  if (value === "live") {
    return "listing-live";
  }
  if (value && value !== "unknown") {
    return "listing-non-live";
  }
  return "listing-unknown-origin";
}

function linkClassName(isValid) {
  return isValid ? "link-valid" : "link-invalid";
}

function listingLinkClassName(isValid) {
  return isValid ? "" : "listing-link-invalid";
}

function normalizeText(value) {
  return nonEmptyText(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

function parseDateValue(value) {
  const timestamp = Date.parse(nonEmptyText(value));
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function addOptionalClasses(element, ...tokens) {
  const validTokens = tokens
    .filter((token) => typeof token === "string")
    .map((token) => token.trim())
    .filter(Boolean);

  if (validTokens.length > 0) {
    element.classList.add(...validTokens);
  }
}

function readPersistedLiveOnlyMode(fallbackValue) {
  try {
    const storedValue = window.sessionStorage.getItem(dashboardModeStorageKey);
    if (storedValue === "1") {
      return true;
    }
    if (storedValue === "0") {
      return false;
    }
  } catch {
    return Boolean(fallbackValue);
  }
  return Boolean(fallbackValue);
}

function persistLiveOnlyMode(value) {
  try {
    window.sessionStorage.setItem(dashboardModeStorageKey, value ? "1" : "0");
  } catch {
    return;
  }
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value
    : {};
}

function hasText(value) {
  return typeof value === "string"
    ? value.trim().length > 0
    : value != null;
}

function nonEmptyText(value, fallback = "") {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed || fallback;
  }
  if (value == null) {
    return fallback;
  }
  const rendered = String(value).trim();
  return rendered || fallback;
}

function toFiniteNumber(value) {
  if (value == null || value === "") {
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function normalizeBooleanLike(value, fallback = false) {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes", "y", "on"].includes(normalized)) {
      return true;
    }
    if (["false", "0", "no", "n", "off", ""].includes(normalized)) {
      return false;
    }
  }
  if (value == null) {
    return Boolean(fallback);
  }
  return Boolean(value);
}
