pub(super) struct Sty {
    pub(super) color: bool,
}

impl Sty {
    fn paint(&self, code: &str, text: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }
    pub(super) fn bold(&self, t: &str) -> String {
        self.paint("1", t)
    }
    pub(super) fn dim(&self, t: &str) -> String {
        self.paint("2", t)
    }
    pub(super) fn red(&self, t: &str) -> String {
        self.paint("31", t)
    }
    pub(super) fn green(&self, t: &str) -> String {
        self.paint("32", t)
    }
    pub(super) fn yellow(&self, t: &str) -> String {
        self.paint("33", t)
    }
    pub(super) fn cyan(&self, t: &str) -> String {
        self.paint("36", t)
    }

    pub(super) fn glyph_pipe(&self) -> &'static str {
        if self.color { "▸" } else { ">" }
    }
    pub(super) fn glyph_stage(&self) -> &'static str {
        if self.color { "●" } else { "*" }
    }
    pub(super) fn glyph_join(&self) -> &'static str {
        if self.color { "⧉" } else { "#" }
    }
    pub(super) fn glyph_dep(&self) -> &'static str {
        if self.color { "◷" } else { "@" }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sty_applies_color_and_glyph_fallbacks() {
        let on = Sty { color: true };
        let off = Sty { color: false };
        assert_eq!(off.green("x"), "x");
        assert!(on.green("x").contains("\x1b[32m"));
        assert_eq!(on.glyph_pipe(), "▸");
        assert_eq!(off.glyph_pipe(), ">");
    }
}
