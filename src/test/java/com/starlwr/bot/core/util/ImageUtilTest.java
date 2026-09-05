package com.starlwr.bot.core.util;

import org.junit.jupiter.api.Test;

import java.awt.Color;
import java.awt.image.BufferedImage;

import static org.assertj.core.api.Assertions.assertThat;

class ImageUtilTest {
    @Test
    void circleMaskKeepsArgbTransparencyOutsideShape() {
        BufferedImage source = solid(20, 20, Color.RED);

        BufferedImage masked = ImageUtil.maskToCircle(source);

        assertThat(masked.getType()).isEqualTo(BufferedImage.TYPE_INT_ARGB);
        assertThat((masked.getRGB(0, 0) >>> 24) & 0xff).isEqualTo(0);
        assertThat((masked.getRGB(10, 10) >>> 24) & 0xff).isEqualTo(255);
    }

    @Test
    void roundedMaskKeepsArgbTransparencyOutsideShape() {
        BufferedImage source = solid(20, 20, Color.BLUE);

        BufferedImage masked = ImageUtil.maskToRoundedRectangle(source, 10);

        assertThat((masked.getRGB(0, 0) >>> 24) & 0xff).isEqualTo(0);
        assertThat((masked.getRGB(10, 10) >>> 24) & 0xff).isEqualTo(255);
    }

    private static BufferedImage solid(int width, int height, Color color) {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        var graphics = image.createGraphics();
        graphics.setColor(color);
        graphics.fillRect(0, 0, width, height);
        graphics.dispose();
        return image;
    }
}
