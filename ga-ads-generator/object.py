class Entity:
    def __init__(self, id, name):
        self.id = id
        self.name = name

    def __hash__(self):
        return hash((self.id, self.name))

    def __eq__(self, other):
        return (self.id, self.name) == (other.id, other.name)

    def to_json(self):
        return {
            "id": self.id,
            "name": self.name
        }


class BevObj(Entity):
    pass


class IntObj(Entity):
    pass


class CorObj:
    def __init__(self, distance_unit, latitude, longitude, radius):
        self.distance_unit = distance_unit
        self.latitude = latitude
        self.longitude = longitude
        self.radius = radius

    def __hash__(self):
        return hash((self.distance_unit, self.latitude, self.longitude, self.radius))

    def __eq__(self, other):
        return (self.distance_unit, self.latitude, self.longitude, self.radius) == \
               (other.distance_unit, other.latitude, other.longitude, other.radius)

    def to_json(self):
        return {"distance_unit": self.distance_unit,
                "latitude": self.latitude,
                "longitude": self.longitude,
                "radius": self.radius}
