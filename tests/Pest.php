<?php

declare(strict_types=1);
uses()->in('Unit', 'Integration', 'Interop');

if (function_exists('typeCoverage')) {
    $tc = typeCoverage();
    if (is_object($tc) && method_exists($tc, 'minimum')) {
        $tc->minimum(85);
    }
}
