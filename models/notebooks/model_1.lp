\Problem name: cost-optimal

Minimize
OBJROW: 0.00128 runtime0 + 0.00113 runtime1 + 0.00199 runtime2 + 0.00192 runtime3 + 0.00301 runtime4 + 0.00108 runtime5 + 0.00151 runtime6 + 0.00128 runtime7 + 0.00168 runtime8 + 0.00741 runtime9
 + 0.00181 runtime10 + 0.00159 runtime11 + 0.00124 runtime12 + 0.00139 runtime13 + 0.00223 runtime14
Subject To
constr(0):  bit00 + bit01 <= 2
constr(1):  bit10 + bit11 <= 2
constr(2):  bit20 + bit21 <= 2
constr(3):  bit30 + bit31 <= 2
constr(4):  bit40 + bit41 <= 2
constr(5):  bit50 + bit51 <= 2
constr(6):  bit60 + bit61 <= 2
constr(7):  bit70 + bit71 <= 2
constr(8):  bit80 + bit81 <= 2
constr(9):  bit90 + bit91 <= 2
constr(10):  bit100 + bit101 <= 2
constr(11):  bit110 + bit111 <= 2
constr(12):  bit120 + bit121 <= 2
constr(13):  bit130 + bit131 <= 2
constr(14):  bit140 + bit141 <= 2
constr(15):  - bit00 + var(105) >= -0
constr(16):  - bit01 + var(105) >= -0
constr(17):  - bit10 + var(106) >= -0
constr(18):  - bit11 + var(106) >= -0
constr(19):  - bit20 + var(107) >= -0
constr(20):  - bit21 + var(107) >= -0
constr(21):  - bit30 + var(108) >= -0
constr(22):  - bit31 + var(108) >= -0
constr(23):  - bit40 + var(109) >= -0
constr(24):  - bit41 + var(109) >= -0
constr(25):  - bit50 + var(110) >= -0
constr(26):  - bit51 + var(110) >= -0
constr(27):  - bit60 + var(111) >= -0
constr(28):  - bit61 + var(111) >= -0
constr(29):  - bit70 + var(112) >= -0
constr(30):  - bit71 + var(112) >= -0
constr(31):  - bit80 + var(113) >= -0
constr(32):  - bit81 + var(113) >= -0
constr(33):  - bit90 + var(114) >= -0
constr(34):  - bit91 + var(114) >= -0
constr(35):  - bit100 + var(115) >= -0
constr(36):  - bit101 + var(115) >= -0
constr(37):  - bit110 + var(116) >= -0
constr(38):  - bit111 + var(116) >= -0
constr(39):  - bit120 + var(117) >= -0
constr(40):  - bit121 + var(117) >= -0
constr(41):  - bit130 + var(118) >= -0
constr(42):  - bit131 + var(118) >= -0
constr(43):  - bit140 + var(119) >= -0
constr(44):  - bit141 + var(119) >= -0
constr(45):  - bit00 - bit01 + var(105) <= -0
constr(46):  - bit10 - bit11 + var(106) <= -0
constr(47):  - bit20 - bit21 + var(107) <= -0
constr(48):  - bit30 - bit31 + var(108) <= -0
constr(49):  - bit40 - bit41 + var(109) <= -0
constr(50):  - bit50 - bit51 + var(110) <= -0
constr(51):  - bit60 - bit61 + var(111) <= -0
constr(52):  - bit70 - bit71 + var(112) <= -0
constr(53):  - bit80 - bit81 + var(113) <= -0
constr(54):  - bit90 - bit91 + var(114) <= -0
constr(55):  - bit100 - bit101 + var(115) <= -0
constr(56):  - bit110 - bit111 + var(116) <= -0
constr(57):  - bit120 - bit121 + var(117) <= -0
constr(58):  - bit130 - bit131 + var(118) <= -0
constr(59):  - bit140 - bit141 + var(119) <= -0
constr(60):  var(105) + var(106) + var(107) + var(108) + var(109) + var(110) + var(111) + var(112) + var(113) + var(114)
 + var(115) + var(116) + var(117) + var(118) + var(119) <= 1
constr(61):  bit00 + bit10 + bit20 + bit30 + bit40 + bit50 + bit60 + bit70 + bit80 + bit90
 + bit100 + bit110 + bit120 + bit130 + bit140 = 1
constr(62):  bit01 + bit11 + bit21 + bit31 + bit41 + bit51 + bit61 + bit71 + bit81 + bit91
 + bit101 + bit111 + bit121 + bit131 + bit141 = 1
constr(63):  3 bit00 + 3 bit01 <= 48
constr(64):  3 bit10 + 3 bit11 <= 48
constr(65):  3 bit20 + 3 bit21 <= 48
constr(66):  3 bit30 + 3 bit31 <= 48
constr(67):  3 bit40 + 3 bit41 <= 48
constr(68):  3 bit50 + 3 bit51 <= 36
constr(69):  3 bit60 + 3 bit61 <= 48
constr(70):  3 bit70 + 3 bit71 <= 48
constr(71):  3 bit80 + 3 bit81 <= 48
constr(72):  3 bit90 + 3 bit91 <= 64
constr(73):  3 bit100 + 3 bit101 <= 48
constr(74):  3 bit110 + 3 bit111 <= 48
constr(75):  3 bit120 + 3 bit121 <= 24
constr(76):  3 bit130 + 3 bit131 <= 32
constr(77):  3 bit140 + 3 bit141 <= 48
constr(78):  192 bit00 + 6 bit01 <= 384
constr(79):  192 bit10 + 6 bit11 <= 192
constr(80):  192 bit20 + 6 bit21 <= 768
constr(81):  192 bit30 + 6 bit31 <= 768
constr(82):  192 bit40 + 6 bit41 <= 768
constr(83):  192 bit50 + 6 bit51 <= 192
constr(84):  192 bit60 + 6 bit61 <= 384
constr(85):  192 bit70 + 6 bit71 <= 192
constr(86):  192 bit80 + 6 bit81 <= 768
constr(87):  192 bit90 + 6 bit91 <= 3904
constr(88):  192 bit100 + 6 bit101 <= 384
constr(89):  192 bit110 + 6 bit111 <= 384
constr(90):  192 bit120 + 6 bit121 <= 384
constr(91):  192 bit130 + 6 bit131 <= 488
constr(92):  192 bit140 + 6 bit141 <= 768
constr(93):  <= -0
constr(94):  <= -0
constr(95):  <= -0
constr(96):  <= 3600
constr(97):  <= 60000
constr(98):  <= -0
constr(99):  <= 3600
constr(100):  <= 3600
constr(101):  <= -0
constr(102):  <= 3840
constr(103):  <= 3600
constr(104):  <= -0
constr(105):  <= 1800
constr(106):  <= 15200
constr(107):  <= 3600
constr(108):  2400 bit00 - runtime0 + 234.84280 var(45) + 234.84280 var(46) <= -0
constr(109):  36000 bit01 - runtime0 + 0.14000 var(47) + 0.14000 var(48) <= -0
constr(110):  2400 bit10 - runtime1 + 234.84280 var(49) + 234.84280 var(50) <= -0
constr(111):  36000 bit11 - runtime1 + 0.14000 var(51) + 0.14000 var(52) <= -0
constr(112):  2400 bit20 - runtime2 + 67.12480 var(53) + 67.12480 var(54) <= -0
constr(113):  36000 bit21 - runtime2 + 0.14000 var(55) + 0.14000 var(56) <= -0
constr(114):  2400 bit30 - runtime3 + 234.84280 var(57) + 234.84280 var(58) <= -0
constr(115):  36000 bit31 - runtime3 + 0.14000 var(59) + 0.14000 var(60) <= -0
constr(116):  2400 bit40 - runtime4 + 67.12480 var(61) + 67.12480 var(62) <= -0
constr(117):  36000 bit41 - runtime4 + 0.14000 var(63) + 0.14000 var(64) <= -0
constr(118):  2400 bit50 - runtime5 + 67.12480 var(65) + 67.12480 var(66) <= -0
constr(119):  36000 bit51 - runtime5 + 0.14000 var(67) + 0.14000 var(68) <= -0
constr(120):  2400 bit60 - runtime6 + 234.84280 var(69) + 234.84280 var(70) <= -0
constr(121):  36000 bit61 - runtime6 + 0.14000 var(71) + 0.14000 var(72) <= -0
constr(122):  2400 bit70 - runtime7 + 234.84280 var(73) + 234.84280 var(74) <= -0
constr(123):  36000 bit71 - runtime7 + 0.14000 var(75) + 0.14000 var(76) <= -0
constr(124):  2400 bit80 - runtime8 + 234.84280 var(77) + 234.84280 var(78) <= -0
constr(125):  36000 bit81 - runtime8 + 0.14000 var(79) + 0.14000 var(80) <= -0
constr(126):  2400 bit90 - runtime9 + 234.84280 var(81) + 234.84280 var(82) <= -0
constr(127):  36000 bit91 - runtime9 + 0.14000 var(83) + 0.14000 var(84) <= -0
constr(128):  2400 bit100 - runtime10 + 67.12480 var(85) + 67.12480 var(86) <= -0
constr(129):  36000 bit101 - runtime10 + 0.14000 var(87) + 0.14000 var(88) <= -0
constr(130):  2400 bit110 - runtime11 + 67.12480 var(89) + 67.12480 var(90) <= -0
constr(131):  36000 bit111 - runtime11 + 0.14000 var(91) + 0.14000 var(92) <= -0
constr(132):  2400 bit120 - runtime12 + 234.84280 var(93) + 234.84280 var(94) <= -0
constr(133):  36000 bit121 - runtime12 + 0.14000 var(95) + 0.14000 var(96) <= -0
constr(134):  2400 bit130 - runtime13 + 234.84280 var(97) + 234.84280 var(98) <= -0
constr(135):  36000 bit131 - runtime13 + 0.14000 var(99) + 0.14000 var(100) <= -0
constr(136):  2400 bit140 - runtime14 + 67.12480 var(101) + 67.12480 var(102) <= -0
constr(137):  36000 bit141 - runtime14 + 0.14000 var(103) + 0.14000 var(104) <= -0
constr(138):  - bit00 + var(45) <= -0
constr(139):  - bit00 + var(45) <= -0
constr(140):  - bit00 + var(45) >= -1
constr(141):  - bit00 + var(46) <= -0
constr(142):  - bit01 + var(46) <= -0
constr(143):  - bit00 - bit01 + var(46) >= -1
constr(144):  - bit01 + var(47) <= -0
constr(145):  - bit00 + var(47) <= -0
constr(146):  - bit00 - bit01 + var(47) >= -1
constr(147):  - bit01 + var(48) <= -0
constr(148):  - bit01 + var(48) <= -0
constr(149):  - bit01 + var(48) >= -1
constr(150):  - bit10 + var(49) <= -0
constr(151):  - bit10 + var(49) <= -0
constr(152):  - bit10 + var(49) >= -1
constr(153):  - bit10 + var(50) <= -0
constr(154):  - bit11 + var(50) <= -0
constr(155):  - bit10 - bit11 + var(50) >= -1
constr(156):  - bit11 + var(51) <= -0
constr(157):  - bit10 + var(51) <= -0
constr(158):  - bit10 - bit11 + var(51) >= -1
constr(159):  - bit11 + var(52) <= -0
constr(160):  - bit11 + var(52) <= -0
constr(161):  - bit11 + var(52) >= -1
constr(162):  - bit20 + var(53) <= -0
constr(163):  - bit20 + var(53) <= -0
constr(164):  - bit20 + var(53) >= -1
constr(165):  - bit20 + var(54) <= -0
constr(166):  - bit21 + var(54) <= -0
constr(167):  - bit20 - bit21 + var(54) >= -1
constr(168):  - bit21 + var(55) <= -0
constr(169):  - bit20 + var(55) <= -0
constr(170):  - bit20 - bit21 + var(55) >= -1
constr(171):  - bit21 + var(56) <= -0
constr(172):  - bit21 + var(56) <= -0
constr(173):  - bit21 + var(56) >= -1
constr(174):  - bit30 + var(57) <= -0
constr(175):  - bit30 + var(57) <= -0
constr(176):  - bit30 + var(57) >= -1
constr(177):  - bit30 + var(58) <= -0
constr(178):  - bit31 + var(58) <= -0
constr(179):  - bit30 - bit31 + var(58) >= -1
constr(180):  - bit31 + var(59) <= -0
constr(181):  - bit30 + var(59) <= -0
constr(182):  - bit30 - bit31 + var(59) >= -1
constr(183):  - bit31 + var(60) <= -0
constr(184):  - bit31 + var(60) <= -0
constr(185):  - bit31 + var(60) >= -1
constr(186):  - bit40 + var(61) <= -0
constr(187):  - bit40 + var(61) <= -0
constr(188):  - bit40 + var(61) >= -1
constr(189):  - bit40 + var(62) <= -0
constr(190):  - bit41 + var(62) <= -0
constr(191):  - bit40 - bit41 + var(62) >= -1
constr(192):  - bit41 + var(63) <= -0
constr(193):  - bit40 + var(63) <= -0
constr(194):  - bit40 - bit41 + var(63) >= -1
constr(195):  - bit41 + var(64) <= -0
constr(196):  - bit41 + var(64) <= -0
constr(197):  - bit41 + var(64) >= -1
constr(198):  - bit50 + var(65) <= -0
constr(199):  - bit50 + var(65) <= -0
constr(200):  - bit50 + var(65) >= -1
constr(201):  - bit50 + var(66) <= -0
constr(202):  - bit51 + var(66) <= -0
constr(203):  - bit50 - bit51 + var(66) >= -1
constr(204):  - bit51 + var(67) <= -0
constr(205):  - bit50 + var(67) <= -0
constr(206):  - bit50 - bit51 + var(67) >= -1
constr(207):  - bit51 + var(68) <= -0
constr(208):  - bit51 + var(68) <= -0
constr(209):  - bit51 + var(68) >= -1
constr(210):  - bit60 + var(69) <= -0
constr(211):  - bit60 + var(69) <= -0
constr(212):  - bit60 + var(69) >= -1
constr(213):  - bit60 + var(70) <= -0
constr(214):  - bit61 + var(70) <= -0
constr(215):  - bit60 - bit61 + var(70) >= -1
constr(216):  - bit61 + var(71) <= -0
constr(217):  - bit60 + var(71) <= -0
constr(218):  - bit60 - bit61 + var(71) >= -1
constr(219):  - bit61 + var(72) <= -0
constr(220):  - bit61 + var(72) <= -0
constr(221):  - bit61 + var(72) >= -1
constr(222):  - bit70 + var(73) <= -0
constr(223):  - bit70 + var(73) <= -0
constr(224):  - bit70 + var(73) >= -1
constr(225):  - bit70 + var(74) <= -0
constr(226):  - bit71 + var(74) <= -0
constr(227):  - bit70 - bit71 + var(74) >= -1
constr(228):  - bit71 + var(75) <= -0
constr(229):  - bit70 + var(75) <= -0
constr(230):  - bit70 - bit71 + var(75) >= -1
constr(231):  - bit71 + var(76) <= -0
constr(232):  - bit71 + var(76) <= -0
constr(233):  - bit71 + var(76) >= -1
constr(234):  - bit80 + var(77) <= -0
constr(235):  - bit80 + var(77) <= -0
constr(236):  - bit80 + var(77) >= -1
constr(237):  - bit80 + var(78) <= -0
constr(238):  - bit81 + var(78) <= -0
constr(239):  - bit80 - bit81 + var(78) >= -1
constr(240):  - bit81 + var(79) <= -0
constr(241):  - bit80 + var(79) <= -0
constr(242):  - bit80 - bit81 + var(79) >= -1
constr(243):  - bit81 + var(80) <= -0
constr(244):  - bit81 + var(80) <= -0
constr(245):  - bit81 + var(80) >= -1
constr(246):  - bit90 + var(81) <= -0
constr(247):  - bit90 + var(81) <= -0
constr(248):  - bit90 + var(81) >= -1
constr(249):  - bit90 + var(82) <= -0
constr(250):  - bit91 + var(82) <= -0
constr(251):  - bit90 - bit91 + var(82) >= -1
constr(252):  - bit91 + var(83) <= -0
constr(253):  - bit90 + var(83) <= -0
constr(254):  - bit90 - bit91 + var(83) >= -1
constr(255):  - bit91 + var(84) <= -0
constr(256):  - bit91 + var(84) <= -0
constr(257):  - bit91 + var(84) >= -1
constr(258):  - bit100 + var(85) <= -0
constr(259):  - bit100 + var(85) <= -0
constr(260):  - bit100 + var(85) >= -1
constr(261):  - bit100 + var(86) <= -0
constr(262):  - bit101 + var(86) <= -0
constr(263):  - bit100 - bit101 + var(86) >= -1
constr(264):  - bit101 + var(87) <= -0
constr(265):  - bit100 + var(87) <= -0
constr(266):  - bit100 - bit101 + var(87) >= -1
constr(267):  - bit101 + var(88) <= -0
constr(268):  - bit101 + var(88) <= -0
constr(269):  - bit101 + var(88) >= -1
constr(270):  - bit110 + var(89) <= -0
constr(271):  - bit110 + var(89) <= -0
constr(272):  - bit110 + var(89) >= -1
constr(273):  - bit110 + var(90) <= -0
constr(274):  - bit111 + var(90) <= -0
constr(275):  - bit110 - bit111 + var(90) >= -1
constr(276):  - bit111 + var(91) <= -0
constr(277):  - bit110 + var(91) <= -0
constr(278):  - bit110 - bit111 + var(91) >= -1
constr(279):  - bit111 + var(92) <= -0
constr(280):  - bit111 + var(92) <= -0
constr(281):  - bit111 + var(92) >= -1
constr(282):  - bit120 + var(93) <= -0
constr(283):  - bit120 + var(93) <= -0
constr(284):  - bit120 + var(93) >= -1
constr(285):  - bit120 + var(94) <= -0
constr(286):  - bit121 + var(94) <= -0
constr(287):  - bit120 - bit121 + var(94) >= -1
constr(288):  - bit121 + var(95) <= -0
constr(289):  - bit120 + var(95) <= -0
constr(290):  - bit120 - bit121 + var(95) >= -1
constr(291):  - bit121 + var(96) <= -0
constr(292):  - bit121 + var(96) <= -0
constr(293):  - bit121 + var(96) >= -1
constr(294):  - bit130 + var(97) <= -0
constr(295):  - bit130 + var(97) <= -0
constr(296):  - bit130 + var(97) >= -1
constr(297):  - bit130 + var(98) <= -0
constr(298):  - bit131 + var(98) <= -0
constr(299):  - bit130 - bit131 + var(98) >= -1
constr(300):  - bit131 + var(99) <= -0
constr(301):  - bit130 + var(99) <= -0
constr(302):  - bit130 - bit131 + var(99) >= -1
constr(303):  - bit131 + var(100) <= -0
constr(304):  - bit131 + var(100) <= -0
constr(305):  - bit131 + var(100) >= -1
constr(306):  - bit140 + var(101) <= -0
constr(307):  - bit140 + var(101) <= -0
constr(308):  - bit140 + var(101) >= -1
constr(309):  - bit140 + var(102) <= -0
constr(310):  - bit141 + var(102) <= -0
constr(311):  - bit140 - bit141 + var(102) >= -1
constr(312):  - bit141 + var(103) <= -0
constr(313):  - bit140 + var(103) <= -0
constr(314):  - bit140 - bit141 + var(103) >= -1
constr(315):  - bit141 + var(104) <= -0
constr(316):  - bit141 + var(104) <= -0
constr(317):  - bit141 + var(104) >= -1
Bounds
 0 <= bit00 <= 1
 0 <= bit01 <= 1
 0 <= bit10 <= 1
 0 <= bit11 <= 1
 0 <= bit20 <= 1
 0 <= bit21 <= 1
 0 <= bit30 <= 1
 0 <= bit31 <= 1
 0 <= bit40 <= 1
 0 <= bit41 <= 1
 0 <= bit50 <= 1
 0 <= bit51 <= 1
 0 <= bit60 <= 1
 0 <= bit61 <= 1
 0 <= bit70 <= 1
 0 <= bit71 <= 1
 0 <= bit80 <= 1
 0 <= bit81 <= 1
 0 <= bit90 <= 1
 0 <= bit91 <= 1
 0 <= bit100 <= 1
 0 <= bit101 <= 1
 0 <= bit110 <= 1
 0 <= bit111 <= 1
 0 <= bit120 <= 1
 0 <= bit121 <= 1
 0 <= bit130 <= 1
 0 <= bit131 <= 1
 0 <= bit140 <= 1
 0 <= bit141 <= 1
 0 <= var(45) <= 1
 0 <= var(46) <= 1
 0 <= var(47) <= 1
 0 <= var(48) <= 1
 0 <= var(49) <= 1
 0 <= var(50) <= 1
 0 <= var(51) <= 1
 0 <= var(52) <= 1
 0 <= var(53) <= 1
 0 <= var(54) <= 1
 0 <= var(55) <= 1
 0 <= var(56) <= 1
 0 <= var(57) <= 1
 0 <= var(58) <= 1
 0 <= var(59) <= 1
 0 <= var(60) <= 1
 0 <= var(61) <= 1
 0 <= var(62) <= 1
 0 <= var(63) <= 1
 0 <= var(64) <= 1
 0 <= var(65) <= 1
 0 <= var(66) <= 1
 0 <= var(67) <= 1
 0 <= var(68) <= 1
 0 <= var(69) <= 1
 0 <= var(70) <= 1
 0 <= var(71) <= 1
 0 <= var(72) <= 1
 0 <= var(73) <= 1
 0 <= var(74) <= 1
 0 <= var(75) <= 1
 0 <= var(76) <= 1
 0 <= var(77) <= 1
 0 <= var(78) <= 1
 0 <= var(79) <= 1
 0 <= var(80) <= 1
 0 <= var(81) <= 1
 0 <= var(82) <= 1
 0 <= var(83) <= 1
 0 <= var(84) <= 1
 0 <= var(85) <= 1
 0 <= var(86) <= 1
 0 <= var(87) <= 1
 0 <= var(88) <= 1
 0 <= var(89) <= 1
 0 <= var(90) <= 1
 0 <= var(91) <= 1
 0 <= var(92) <= 1
 0 <= var(93) <= 1
 0 <= var(94) <= 1
 0 <= var(95) <= 1
 0 <= var(96) <= 1
 0 <= var(97) <= 1
 0 <= var(98) <= 1
 0 <= var(99) <= 1
 0 <= var(100) <= 1
 0 <= var(101) <= 1
 0 <= var(102) <= 1
 0 <= var(103) <= 1
 0 <= var(104) <= 1
 0 <= var(105) <= 1
 0 <= var(106) <= 1
 0 <= var(107) <= 1
 0 <= var(108) <= 1
 0 <= var(109) <= 1
 0 <= var(110) <= 1
 0 <= var(111) <= 1
 0 <= var(112) <= 1
 0 <= var(113) <= 1
 0 <= var(114) <= 1
 0 <= var(115) <= 1
 0 <= var(116) <= 1
 0 <= var(117) <= 1
 0 <= var(118) <= 1
 0 <= var(119) <= 1
Integers
bit00 bit01 bit10 bit11 bit20 bit21 bit30 bit31 bit40 bit41 
bit50 bit51 bit60 bit61 bit70 bit71 bit80 bit81 bit90 bit91 
bit100 bit101 bit110 bit111 bit120 bit121 bit130 bit131 bit140 bit141 
var(45) var(46) var(47) var(48) var(49) var(50) var(51) var(52) var(53) var(54) 
var(55) var(56) var(57) var(58) var(59) var(60) var(61) var(62) var(63) var(64) 
var(65) var(66) var(67) var(68) var(69) var(70) var(71) var(72) var(73) var(74) 
var(75) var(76) var(77) var(78) var(79) var(80) var(81) var(82) var(83) var(84) 
var(85) var(86) var(87) var(88) var(89) var(90) var(91) var(92) var(93) var(94) 
var(95) var(96) var(97) var(98) var(99) var(100) var(101) var(102) var(103) var(104) 
var(105) var(106) var(107) var(108) var(109) var(110) var(111) var(112) var(113) var(114) 
var(115) var(116) var(117) var(118) var(119) 
End
