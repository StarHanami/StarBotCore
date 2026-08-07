package com.starlwr.bot.core.converter;

import org.junit.jupiter.api.Test;

import java.awt.Color;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ColorConverterTest {
    private final ColorConverter converter = new ColorConverter();

    @Test
    void convertsArgbHexColor() {
        Color color = converter.convert("#D9FB7299");

        assertEquals(251, color.getRed());
        assertEquals(114, color.getGreen());
        assertEquals(153, color.getBlue());
        assertEquals(217, color.getAlpha());
    }
}
