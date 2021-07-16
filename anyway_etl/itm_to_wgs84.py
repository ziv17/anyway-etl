from pyproj import Transformer


class ItmToWGS84(object):
    def __init__(self):
        # initializing WGS84 (epsg: 4326) and Israeli TM Grid (epsg: 2039) projections.
        # for more info: https://epsg.io/<epsg_num>/
        self.transformer = Transformer.from_proj(2039, 4326, always_xy=True)

    def convert(self, x, y):
        """
        converts ITM to WGS84 coordinates
        :type x: float
        :type y: float
        :rtype: tuple
        :return: (longitude,latitude)
        """
        longitude, latitude = self.transformer.transform(x, y)
        return longitude, latitude
