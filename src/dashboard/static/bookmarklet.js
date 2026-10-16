// Hardware Pulse bookmarklet: reads the product of the store page the user is
// looking at (Amazon, or any store with schema.org JSON-LD such as Newegg,
// B&H or Best Buy) and opens the import calculator with it. It only runs when
// the user clicks it, in their own browser: nothing is scraped by a server.
//
// src/dashboard/importing.py turns this file into a "javascript:" link: lines
// that start with "//" are dropped and __APP_URL__ is replaced by the app URL.
// Every statement ends with ";" because the link is a single line.
// With window.__HP_TEST__ set, it exposes window.hpBookmarklet instead of
// opening a tab (tests/e2e).
(function () {
  var APP = "__APP_URL__";

  function clean(value) {
    return String(value || "").replace(/[‎‏ ]/g, " ").replace(/\s+/g, " ").trim();
  }

  function text(selector) {
    var el = document.querySelector(selector);
    return el ? clean(el.textContent) : "";
  }

  function first(selectors) {
    for (var i = 0; i < selectors.length; i++) {
      var value = text(selectors[i]);
      if (value) {
        return value;
      }
    }
    return "";
  }

  function startsWithAny(label, prefixes) {
    for (var i = 0; i < prefixes.length; i++) {
      if (label.indexOf(prefixes[i]) === 0) {
        return true;
      }
    }
    return false;
  }

  function detail(prefixes) {
    var rows = document.querySelectorAll(
      "#productDetails_techSpec_section_1 tr, #productDetails_detailBullets_sections1 tr, " +
        "#technicalSpecifications_section_1 tr, #productDetails_feature_div tr, .prodDetTable tr"
    );
    for (var i = 0; i < rows.length; i++) {
      var head = rows[i].querySelector("th");
      var cell = rows[i].querySelector("td");
      if (head && cell && startsWithAny(clean(head.textContent).toLowerCase(), prefixes)) {
        return clean(cell.textContent);
      }
    }
    var bullets = document.querySelectorAll(
      "#detailBullets_feature_div li, #detailBulletsWrapper_feature_div li"
    );
    for (var j = 0; j < bullets.length; j++) {
      var line = clean(bullets[j].textContent);
      var colon = line.indexOf(":");
      if (colon > 0 && startsWithAny(clean(line.slice(0, colon)).toLowerCase(), prefixes)) {
        return clean(line.slice(colon + 1));
      }
    }
    return "";
  }

  function amazon() {
    return {
      t: text("#productTitle"),
      p: first([
        "#corePrice_feature_div .a-price .a-offscreen",
        "#corePriceDisplay_desktop_feature_div .a-price .a-offscreen",
        "#apex_desktop .a-price .a-offscreen",
        "#price_inside_buybox",
        "#priceblock_ourprice",
        "#buybox .a-price .a-offscreen"
      ]),
      sold: first([
        "#merchantInfoFeature_feature_div .offer-display-feature-text-message",
        ".tabular-buybox-text[tabular-attribute-name='Sold by'] span",
        "#sellerProfileTriggerId",
        "#merchant-info"
      ]),
      w: detail(["item weight", "peso del producto", "peso del artículo", "peso"]),
      d: detail([
        "package dimensions",
        "dimensiones del paquete",
        "product dimensions",
        "dimensiones del producto"
      ]),
      mpn: detail([
        "item model number",
        "manufacturer part number",
        "número de modelo",
        "numero de modelo"
      ])
    };
  }

  function productFromJsonLd() {
    var scripts = document.querySelectorAll("script[type='application/ld+json']");
    for (var i = 0; i < scripts.length; i++) {
      var data;
      try {
        data = JSON.parse(scripts[i].textContent);
      } catch (error) {
        continue;
      }
      var nodes = Array.isArray(data) ? data : data["@graph"] || [data];
      for (var j = 0; j < nodes.length; j++) {
        var type = nodes[j] && nodes[j]["@type"];
        if (type === "Product" || (Array.isArray(type) && type.indexOf("Product") >= 0)) {
          return nodes[j];
        }
      }
    }
    return null;
  }

  function generic() {
    var product = productFromJsonLd() || {};
    var offer = product.offers || {};
    if (Array.isArray(offer)) {
      offer = offer[0] || {};
    }
    if (offer.offers) {
      offer = Array.isArray(offer.offers) ? offer.offers[0] || {} : offer.offers;
    }
    var amount = offer.price || offer.lowPrice || "";
    var currency = offer.priceCurrency || "USD";
    var weight = product.weight || "";
    if (weight && typeof weight === "object") {
      weight = (weight.value || "") + " " + (weight.unitText || weight.unitCode || "");
    }
    return {
      t: clean(product.name || document.title),
      p: amount ? (currency === "USD" ? "$" : currency + " ") + amount : "",
      sold: clean(offer.seller && offer.seller.name),
      w: clean(weight),
      d: "",
      mpn: clean(product.mpn || product.sku || "")
    };
  }

  function extract(location) {
    var data = /(^|\.)amazon\.com$/.test(location.hostname) ? amazon() : generic();
    data.u = location.origin + location.pathname;
    if (!data.p && !window.__HP_TEST__) {
      var typed = window.prompt("No encontré el precio. Ingresalo en dólares (ej. 199.99):");
      if (typed) {
        data.p = "$" + typed;
      }
    }
    return data;
  }

  function buildUrl(data) {
    var params = ["src=bm"];
    var keys = ["t", "p", "u", "sold", "w", "d", "mpn"];
    for (var i = 0; i < keys.length; i++) {
      var value = data[keys[i]];
      if (value) {
        params.push(keys[i] + "=" + encodeURIComponent(String(value).slice(0, 300)));
      }
    }
    return APP + "/importar?" + params.join("&");
  }

  if (window.__HP_TEST__) {
    window.hpBookmarklet = { extract: extract, buildUrl: buildUrl };
    return;
  }
  window.open(buildUrl(extract(window.location)), "_blank");
})();
