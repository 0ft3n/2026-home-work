counter = 0

local keys = {}
local f = io.open("/data/generated_keys.txt", "r")
if f then
    for line in f:lines() do
        keys[#keys + 1] = line
    end
    f:close()
end

if #keys == 0 then
    error("No keys found in /data/generated_keys.txt")
end

request = function()
    local key = keys[math.random(#keys)]
    return wrk.format("GET", "/v0/entity?id=" .. key)
end

